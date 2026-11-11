// Winning-ticket store operations follow livepeer/go-livepeer/common/db.go,
// by Elad Mallel and Nico Vergauwen (404d24a9455cd0997af1be6d18faa998702cce69).
// The redemption recovery schema and transaction state are implemented here.
package redemption

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var MigrationFiles embed.FS

// RedeemerDB owns the shared durable winning-ticket queue,
// redemption lifecycle, and chain activity observations.
type RedeemerDB struct {
	logger         *slog.Logger
	metrics        *observability.Metrics
	snapshot       func() (eth.ChainSnapshot, bool)
	db             *sql.DB
	admission      *sql.Stmt
	recoveryMu     sync.Mutex
	recovery       *redemptionRecovery
	binding        *Binding
	limits         Limits
	receiptCursor  scanCursor
	recoveryCursor scanCursor
	path           string
}

// Each sweep has a fixed end, so new transactions cannot indefinitely postpone
// older work. Advance only when processing reaches a row, before its RPC work.
type scanCursor struct{ after, through atomic.Int64 }

func (c *scanCursor) bounds(ctx context.Context, db *sql.DB) (int64, int64, error) {
	end := c.through.Load()
	if end == 0 {
		if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(rowid),0) FROM redemption_transactions").Scan(&end); err != nil {
			return 0, 0, err
		}
		c.through.Store(end)
	}
	return c.after.Load(), end, nil
}

func (c *scanCursor) finish(exhausted bool) {
	if exhausted || c.after.Load() >= c.through.Load() {
		c.after.Store(0)
		c.through.Store(0)
	}
}

func OpenRedeemerDB(path string) (*RedeemerDB, error) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, path, true)
	if err != nil {
		return nil, err
	}
	return NewStore(db)
}

// NewStore takes ownership of db and initializes the shared migration catalog.
func NewStore(db *sql.DB) (*RedeemerDB, error) {
	ctx := context.Background()
	var err error
	files, err := fs.Sub(MigrationFiles, "migrations")
	if err == nil {
		err = migrations.Up(ctx, db, files)
	}
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize redeemer SQLite: %w", err)
	}
	store := &RedeemerDB{db: db, limits: DefaultLimits}
	var seq int
	var name string
	if err := db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&seq, &name, &store.path); err != nil {
		_ = db.Close()
		return nil, err
	}
	store.admission, err = db.PrepareContext(ctx, receiptAdmissionQuery)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("prepare payment admission checks: %w", err)
	}

	if err := store.backfill(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if b, e := store.Binding(ctx); e == nil {
		store.binding = &b
	} else if !errors.Is(e, sql.ErrNoRows) {
		_ = db.Close()
		return nil, e
	}
	return store, nil
}

func OpenDatabase(ctx context.Context, path string, create bool) (*sql.DB, error) {
	if path == "" {
		return nil, errors.New("redeemer SQLite path is required")
	}
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("redeemer SQLite state requires a filesystem path")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	flags := os.O_RDWR
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, errors.New("redeemer SQLite file is unavailable")
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("redeemer SQLite file must be owner-only")
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, stmt := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("initialize redeemer SQLite: %w", err)
		}
	}
	return db, nil
}

func (s *RedeemerDB) Close() error {
	s.recoveryMu.Lock()
	r := s.recovery
	s.recoveryMu.Unlock()
	if r != nil {
		r.close()
	}
	return s.db.Close()
}

func (s *RedeemerDB) StoreWinningTicket(t *pm.SignedTicket) error {
	if err := validStoredTicket(t); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.capacity(context.Background(), tx, t.Recipient, 1); err != nil {
		return err
	}
	if err = insertTicket(context.Background(), tx, t); err != nil {
		return err
	}
	return tx.Commit()
}

const receiptAdmissionQuery = `SELECT m.number,m.hash,EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE w.recipient_rand_hash=?1 AND (a.phase!='expired' OR EXISTS(SELECT 1 FROM redemption_transactions t WHERE t.sig=w.sig)))
 OR EXISTS(SELECT 1 FROM winning_tickets w JOIN ticket_state t ON t.hash=w.ticket_hash WHERE w.recipient_rand_hash=?1 AND t.exposed=1)
 FROM (SELECT 1) LEFT JOIN collateral_watermark m ON m.id=1`

// StoreWinningTickets commits a validated receipt batch before the payment
// engine publishes replay guards or credit. Check exposure and liability in the
// same transaction so redemption and concurrent receipts cannot race these gates.
func (s *RedeemerDB) StoreWinningTickets(ctx context.Context, payer ethcommon.Address, randHash ethcommon.Hash, collateral *big.Int, tickets []*pm.SignedTicket, snapshot pm.ChainSnapshot) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// A singleton durable floor prevents old collateral being reused after any
	// settlement releases liability. It is deliberately conservative across payers.
	var number sql.NullInt64
	var hash sql.NullString
	var exposed int
	if err := tx.StmtContext(ctx, s.admission).QueryRowContext(ctx, randHash.Hex()).Scan(&number, &hash, &exposed); err != nil {
		return err
	}
	if number.Valid && (snapshot.Number == nil || snapshot.Number.Cmp(big.NewInt(number.Int64)) < 0 || (snapshot.Number.Cmp(big.NewInt(number.Int64)) == 0 && snapshot.BlockHash.Hex() != hash.String)) {
		return pm.ErrPayerUnavailable
	}
	// Once redemption can reveal randomness, this epoch must never accept
	// more tickets, even if a reorg moves the observed L1 clock backwards.
	if exposed != 0 {
		return ErrExposedRandomness
	}
	if len(tickets) > 0 {
		if err := s.capacity(ctx, tx, tickets[0].Recipient, len(tickets)); err != nil {
			return err
		}
	}
	for _, ticket := range tickets {
		if err := insertTicket(ctx, tx, ticket); err != nil {
			return err
		}
	}
	pending, err := s.unsettledLiability(ctx, tx, payer)
	if err != nil {
		return err
	}
	if pending.Cmp(collateral) > 0 {
		return pm.ErrPayerUnavailable
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	return tx.Commit()
}

func validStoredTicket(t *pm.SignedTicket) error {
	if t == nil || t.Ticket == nil || t.FaceValue == nil || t.WinProb == nil || t.ParamsExpirationBlock == nil || t.RecipientRand == nil || len(t.Sig) == 0 {
		return errors.New("incomplete signed ticket")
	}
	if t.FaceValue.Sign() <= 0 || t.WinProb.Sign() <= 0 || t.ParamsExpirationBlock.Sign() < 0 {
		return errors.New("invalid signed ticket values")
	}
	return nil
}

func (s *RedeemerDB) SelectEarliestWinningTicket(payer ethcommon.Address, minCreationRound int64) (*pm.SignedTicket, error) {
	row := s.db.QueryRow(`SELECT `+qualifiedTicketColumns()+` FROM ticket_state t JOIN winning_tickets w ON w.sig=t.sig WHERE w.payer_address=? AND w.creation_round>=? AND t.settled=0 ORDER BY w.seq LIMIT 1`, payer.Hex(), minCreationRound)

	ticket, err := scanStoredTicket(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return ticket, err
}

func scanStoredTicket(row interface{ Scan(...any) error }) (*pm.SignedTicket, error) {
	var fields ticketFields
	if err := row.Scan(fields.destinations()...); err != nil {
		return nil, err
	}
	return fields.ticket()
}

func (s *RedeemerDB) winningTicket(ctx context.Context, sig []byte) (*pm.SignedTicket, error) {
	row := s.db.QueryRowContext(ctx, `SELECT payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block FROM winning_tickets WHERE sig=?`, sig)
	return scanStoredTicket(row)
}

func (s *RedeemerDB) WinningTicketCount(payer ethcommon.Address) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT count(*) FROM ticket_state t JOIN winning_tickets w ON w.sig=t.sig WHERE w.payer_address=? AND t.settled=0`, payer.Hex()).Scan(&count)
	return count, err
}

type SubmittedRedemption struct {
	Signature []byte
	Hash      ethcommon.Hash
}

func (s *RedeemerDB) SubmittedRedemptions() ([]SubmittedRedemption, error) {
	rows, err := s.submittedRedemptions(context.Background(), false)
	if err != nil {
		return nil, err
	}
	var out []SubmittedRedemption
	for _, row := range rows {
		out = append(out, row.SubmittedRedemption)
	}
	return out, nil
}

type submittedRedemption struct {
	SubmittedRedemption
	rowid int64
}

func (s *RedeemerDB) submittedRedemptions(ctx context.Context, bounded bool) ([]submittedRedemption, error) {
	query := `SELECT t.rowid,t.sig,t.hash FROM redemption_transactions t JOIN redemption_attempts a ON a.sig=t.sig WHERE a.phase NOT IN ('confirmed','used') AND t.phase NOT IN ('confirmed','reverted','superseded')`
	var args []any
	if bounded {
		after, end, err := s.receiptCursor.bounds(ctx, s.db)
		if err != nil {
			return nil, err
		}
		query += " AND t.rowid>? AND t.rowid<=?"
		args = append(args, after, end)
	}
	query += " ORDER BY t.rowid"
	if bounded {
		query += " LIMIT 128"
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []submittedRedemption
	for rows.Next() {
		var id int64
		var item submittedRedemption
		var hash string
		if err = rows.Scan(&id, &item.Signature, &hash); err != nil {
			return nil, err
		}
		if !ethcommon.IsHexHash(hash) {
			return nil, errors.New("corrupt redemption transaction hash")
		}
		item.Hash = ethcommon.HexToHash(hash)
		if bounded {
			item.rowid = id
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

var ErrExposedRandomness = errors.New("ticket randomness is already exposed")

// Configure installs observation hooks before starting workers.
func (s *RedeemerDB) Configure(logger *slog.Logger, metrics *observability.Metrics, snapshot func() (eth.ChainSnapshot, bool)) {
	s.logger, s.metrics, s.snapshot = logger, metrics, snapshot
}
