package redemption

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
)

var ErrCapacity = errors.New("winning-ticket queue capacity exhausted")
var ErrConflict = errors.New("ticket scheduling metadata conflicts with stored ticket")

type Limits struct{ Pending, PerRecipient int }

var DefaultLimits = Limits{Pending: 100000, PerRecipient: 10000}

type Binding struct {
	ChainID           string
	Controller        common.Address
	Role, Destination string
}

type NonceReader interface {
	FinalizedNonce(context.Context, common.Address) (uint64, error)
}

// Bind prevents retained tickets from being reinterpreted on another deployment.
// An unbound Node database is adopted only by the orchestrator and only after
// checking every retained signed transaction's chain identity.
func (s *RedeemerDB) Bind(ctx context.Context, b Binding, limits Limits, chain NonceReader) error {
	chainID, ok := new(big.Int).SetString(b.ChainID, 10)
	if !ok || chainID.Sign() <= 0 || b.Controller == (common.Address{}) || (b.Role != "orchestrator" && b.Role != "redeemer") || b.Destination == "" || limits.Pending <= 0 || limits.PerRecipient <= 0 {
		return errors.New("invalid redemption binding or capacity")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous Binding
	var controller string
	err = tx.QueryRowContext(ctx, "SELECT chain_id,controller,role,destination FROM redemption_binding WHERE id=1").Scan(&previous.ChainID, &controller, &previous.Role, &previous.Destination)
	if err == nil {
		previous.Controller = common.HexToAddress(controller)
		if previous.ChainID != b.ChainID || previous.Controller != b.Controller || previous.Role != b.Role {
			return errors.New("redemption database deployment or application role mismatch")
		}
		if previous.Destination != b.Destination {
			var count int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM ticket_state WHERE settled=0").Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				return errors.New("drain outstanding tickets before changing redemption destination")
			}
			if err := checkNonceDrain(ctx, tx, chain); err != nil {
				return err
			}
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM winning_tickets").Scan(&count); err != nil {
			return err
		}
		if b.Role == "redeemer" && count != 0 {
			return errors.New("standalone redeemer requires its own database")
		}
		if b.Destination != "chain" {
			if err := checkNonceDrain(ctx, tx, chain); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, "SELECT raw_transaction FROM redemption_transactions")
		if err != nil {
			return err
		}
		for rows.Next() {
			var raw []byte
			if err = rows.Scan(&raw); err != nil {
				rows.Close()
				return err
			}
			var signed types.Transaction
			if signed.UnmarshalBinary(raw) != nil || signed.ChainId().Cmp(chainID) != 0 {
				rows.Close()
				return errors.New("stored redemption transaction chain mismatch")
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	} else {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO redemption_binding(id,chain_id,controller,role,destination) VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET destination=excluded.destination", b.ChainID, b.Controller.Hex(), b.Role, b.Destination)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.binding, s.limits = &b, limits
	return nil
}

// Ticket settlement does not consume every signed nonce. Only finalized
// same-nonce inclusion or a finalized account count proves drainage.
func checkNonceDrain(ctx context.Context, tx *sql.Tx, chain NonceReader) error {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT t.redeemer_address,t.nonce FROM redemption_transactions t WHERE NOT EXISTS(SELECT 1 FROM redemption_transactions proof WHERE proof.redeemer_address=t.redeemer_address AND proof.nonce=t.nonce AND proof.phase IN ('confirmed','reverted'))`)
	if err != nil {
		return err
	}
	needed := map[common.Address]uint64{}
	for rows.Next() {
		var sender, raw string
		if err = rows.Scan(&sender, &raw); err != nil {
			break
		}
		nonce, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil || !common.IsHexAddress(sender) {
			err = errors.New("invalid stored redemption account or nonce")
			break
		}
		address := common.HexToAddress(sender)
		needed[address] = max(needed[address], nonce)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for address, nonce := range needed {
		if chain == nil {
			return errors.New("drain local signed transactions before changing redemption destination")
		}
		count, err := chain.FinalizedNonce(ctx, address)
		if err != nil {
			return err
		}
		if count <= nonce {
			return errors.New("drain local signed transactions before changing redemption destination")
		}
	}
	return nil
}

const ticketColumns = "payer_address,recipient,face_value,win_prob,ticket_nonce,recipient_rand,recipient_rand_hash,sig,creation_round,creation_round_block_hash,params_expiration_block"

func (s *RedeemerDB) backfill(ctx context.Context) error {
	for {
		rows, err := s.db.QueryContext(ctx, "SELECT "+ticketColumns+" FROM winning_tickets WHERE ticket_hash IS NULL ORDER BY seq LIMIT 128")
		if err != nil {
			return err
		}
		var tickets []*pm.SignedTicket
		for rows.Next() {
			t, err := scanStoredTicket(rows)
			if err != nil {
				rows.Close()
				return err
			}
			tickets = append(tickets, t)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(tickets) == 0 {
			return nil
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, t := range tickets {
			if err = recordIdentity(ctx, tx, t); err != nil {
				break
			}
		}
		if err != nil {
			tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
}

func recordIdentity(ctx context.Context, tx *sql.Tx, t *pm.SignedTicket) error {
	hash := t.Hash()
	_, err := tx.ExecContext(ctx, "INSERT INTO ticket_state(hash,sig) VALUES(?,?) ON CONFLICT(hash) DO NOTHING", hash.Bytes(), t.Sig)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE winning_tickets SET ticket_hash=? WHERE sig=?", hash.Bytes(), t.Sig)
	if err != nil {
		return err
	}
	// Existing aliases and transaction histories are retained, never rewritten.
	_, err = tx.ExecContext(ctx, `UPDATE ticket_state SET exposed=MAX(exposed,EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE w.ticket_hash=ticket_state.hash AND (a.phase!='expired' OR EXISTS(SELECT 1 FROM redemption_transactions t WHERE t.sig=w.sig)))),settled=MAX(settled,EXISTS(SELECT 1 FROM winning_tickets w WHERE w.ticket_hash=ticket_state.hash AND w.redeemed_at IS NOT NULL)) WHERE hash=?`, hash.Bytes())
	return err
}

func insertTicket(ctx context.Context, tx *sql.Tx, t *pm.SignedTicket) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO winning_tickets(`+ticketColumns+`,enqueued_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, t.PayerAddress.Hex(), t.Recipient.Hex(), t.FaceValue.Bytes(), t.WinProb.Bytes(), t.TicketNonce, common.LeftPadBytes(t.RecipientRand.Bytes(), 32), t.RecipientRandHash.Hex(), t.Sig, t.CreationRound, t.CreationRoundBlockHash.Hex(), t.ParamsExpirationBlock.String(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	return recordIdentity(ctx, tx, t)
}

func (s *RedeemerDB) capacity(ctx context.Context, tx *sql.Tx, recipient common.Address, additional int) error {
	var total, local int
	err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(w.recipient=?),0) FROM ticket_state t JOIN winning_tickets w ON w.sig=t.sig WHERE t.settled=0`, recipient.Hex()).Scan(&total, &local)
	if err != nil {
		return err
	}
	if total+additional > s.limits.Pending || local+additional > s.limits.PerRecipient {
		return ErrCapacity
	}
	return nil
}

// Admit is the untrusted HTTP boundary. Validation also precedes deduplication:
// another signature for the same hash can have a different winning outcome.
func (s *RedeemerDB) Admit(ctx context.Context, t *pm.SignedTicket) error {
	if err := ValidateTicket(t); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stored, err := scanStoredTicket(tx.QueryRowContext(ctx, "SELECT "+ticketColumns+" FROM winning_tickets WHERE sig=(SELECT sig FROM ticket_state WHERE hash=?)", t.Hash().Bytes()))
	if err == nil {
		if stored.ParamsExpirationBlock.Cmp(t.ParamsExpirationBlock) != 0 {
			return ErrConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = s.capacity(ctx, tx, t.Recipient, 1); err != nil {
		return err
	}
	if err = insertTicket(ctx, tx, t); err != nil {
		return err
	}
	return tx.Commit()
}

func ValidateTicket(t *pm.SignedTicket) error {
	if err := validStoredTicket(t); err != nil {
		return err
	}
	if t.Recipient == (common.Address{}) || t.PayerAddress == (common.Address{}) || t.CreationRound < 0 || t.RecipientRand.Sign() < 0 || len(t.Sig) != 65 {
		return errors.New("invalid signed ticket")
	}
	for _, n := range []*big.Int{t.FaceValue, t.WinProb, t.RecipientRand, t.ParamsExpirationBlock} {
		if n.Sign() < 0 || n.BitLen() > 256 {
			return errors.New("ticket integer out of range")
		}
	}
	v := pm.NewValidator(pm.DefaultSigVerifier{})
	if err := v.ValidateTicket(t.Recipient, t.Ticket, t.Sig, t.RecipientRand); err != nil {
		return errors.New("invalid ticket signature or randomness")
	}
	if !v.IsWinningTicket(t.Ticket, t.Sig, t.RecipientRand) {
		return errors.New("ticket did not win")
	}
	return nil
}

func (s *RedeemerDB) settle(ctx context.Context, t *pm.SignedTicket, phase string, at eth.ChainSnapshot) error {
	return s.finishTicket(ctx, t.Sig, phase, "", at)
}

type queuedTicket struct {
	ticket   *pm.SignedTicket
	delivery string
	failures int
}

func (s *RedeemerDB) page(ctx context.Context, limit int) ([]queuedTicket, error) {
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
 SELECT w.seq,t.hash,t.visited,ROW_NUMBER() OVER (PARTITION BY w.payer_address,w.recipient ORDER BY t.visited,w.seq) AS position
 FROM ticket_state t JOIN winning_tickets w ON w.sig=t.sig WHERE t.settled=0 AND t.next_attempt<=?
 ) SELECT `+qualifiedTicketColumns()+`,t.delivery,t.failures FROM candidates c JOIN winning_tickets w ON w.seq=c.seq JOIN ticket_state t ON t.hash=c.hash ORDER BY c.position,c.visited,c.seq LIMIT ?`, time.Now().UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	var out []queuedTicket
	for rows.Next() {
		var d string
		var failures int
		var fields ticketFields
		args := append(fields.destinations(), &d, &failures)
		if err = rows.Scan(args...); err != nil {
			rows.Close()
			return nil, err
		}
		t, e := fields.ticket()
		if e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, queuedTicket{t, d, failures})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return out, err
}

func qualifiedTicketColumns() string {
	return "w.payer_address,w.recipient,w.face_value,w.win_prob,w.ticket_nonce,w.recipient_rand,w.recipient_rand_hash,w.sig,w.creation_round,w.creation_round_block_hash,w.params_expiration_block"
}

func (s *RedeemerDB) delivery(ctx context.Context, t *pm.SignedTicket, phase string, failures int) error {
	delay := time.Duration(0)
	if phase == "retryable" || phase == "held" {
		delay = 5 * time.Second * time.Duration(1<<min(failures, 6))
	}
	if phase == "acknowledged" || phase == "rejected" {
		delay = 15 * time.Second
	}
	_, err := s.db.ExecContext(ctx, "UPDATE ticket_state SET delivery=?,exposed=MAX(exposed,?),failures=?,next_attempt=? WHERE hash=? AND settled=0", phase, phase == "sending" || phase == "acknowledged", failures, time.Now().Add(delay).UnixMilli(), t.Hash().Bytes())
	if err == nil {
		s.observeTransition(phase, "", nil)
	}
	return err
}

func (s *RedeemerDB) unsettledLiability(ctx context.Context, tx *sql.Tx, payer common.Address) (*big.Int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT w.face_value FROM ticket_state t JOIN winning_tickets w ON w.sig=t.sig WHERE w.payer_address=? AND w.redeemed_at IS NULL AND t.settled=0`, payer.Hex())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	amount := new(big.Int)
	for rows.Next() {
		var face []byte
		if err = rows.Scan(&face); err != nil {
			return nil, err
		}
		amount.Add(amount, new(big.Int).SetBytes(face))
	}
	return amount, rows.Err()
}

// ticketFields allows bounded pages and backfills to reuse the store decoder.
type ticketFields struct {
	payer, recipient, randHash, blockHash, expiration string
	face, prob, rand, sig                             []byte
	nonce, round                                      int64
}

func (f *ticketFields) destinations() []any {
	return []any{&f.payer, &f.recipient, &f.face, &f.prob, &f.nonce, &f.rand, &f.randHash, &f.sig, &f.round, &f.blockHash, &f.expiration}
}
func (f *ticketFields) ticket() (*pm.SignedTicket, error) {
	exp, ok := new(big.Int).SetString(f.expiration, 10)
	if !ok || exp.Sign() < 0 || exp.BitLen() > 256 || f.round < 0 || len(f.face) > 32 || len(f.prob) > 32 || len(f.rand) > 32 || f.nonce < 0 || f.nonce > 1<<32-1 || !common.IsHexAddress(f.payer) || !common.IsHexAddress(f.recipient) {
		return nil, errors.New("corrupt winning ticket")
	}
	return &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: common.HexToAddress(f.payer), Recipient: common.HexToAddress(f.recipient), FaceValue: new(big.Int).SetBytes(f.face), WinProb: new(big.Int).SetBytes(f.prob), TicketNonce: uint32(f.nonce), RecipientRandHash: common.HexToHash(f.randHash), CreationRound: f.round, CreationRoundBlockHash: common.HexToHash(f.blockHash), ParamsExpirationBlock: exp}, Sig: bytes.Clone(f.sig), RecipientRand: new(big.Int).SetBytes(f.rand)}, nil
}

// CapacityAvailable reports admission backpressure without evicting retained work.
func (s *RedeemerDB) CapacityAvailable(ctx context.Context, recipient common.Address) (bool, error) {
	var total, local int
	err := s.db.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(w.recipient=?),0) FROM ticket_state t JOIN winning_tickets w ON w.sig=t.sig WHERE t.settled=0`, recipient.Hex()).Scan(&total, &local)
	return err == nil && total < s.limits.Pending && (recipient == (common.Address{}) || local < s.limits.PerRecipient), err
}

func (s *RedeemerDB) postpone(ctx context.Context, t *pm.SignedTicket, delay time.Duration) error {
	_, err := s.db.ExecContext(ctx, "UPDATE ticket_state SET next_attempt=? WHERE hash=? AND settled=0", time.Now().Add(delay).UnixMilli(), t.Hash().Bytes())
	return err
}
