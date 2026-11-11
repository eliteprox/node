package redemption

import (
	"context"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

type receiptReaderFunc func(context.Context, ethcommon.Hash) (*types.Receipt, error)

func (f receiptReaderFunc) FinalizedReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	return f(ctx, hash)
}

func TestNonTransactionSettlementStatus(t *testing.T) {
	for _, mode := range []string{"queued", "acknowledged", "backfilled_used"} {
		t.Run(mode, func(t *testing.T) {
			f := newRecoveryRPC(t)
			s, _ := boundStore(t, "orchestrator", "http://redeemer.example", DefaultLimits)
			ticket, _ := winningTicket(t, 1)
			require.NoError(t, s.Admit(t.Context(), ticket))
			if mode == "acknowledged" {
				require.NoError(t, s.delivery(t.Context(), ticket, "acknowledged", 0))
			}
			finalized, err := f.Chain(t).RedemptionSnapshot(t.Context(), "finalized")
			require.NoError(t, err)
			require.NoError(t, s.settle(t.Context(), ticket, "used", finalized))
			if mode == "backfilled_used" {
				// Retain the legacy used summary and redeemed_at, then rebuild
				// the additive hash/settlement state as an upgrade would.
				_, err = s.db.Exec("UPDATE winning_tickets SET ticket_hash=NULL")
				require.NoError(t, err)
				_, err = s.db.Exec("DELETE FROM ticket_state")
				require.NoError(t, err)
				require.NoError(t, s.backfill(t.Context()))
			}
			statuses, err := s.Redemptions()
			require.NoError(t, err)
			require.Equal(t, []RedemptionStatus{{Hash: ticket.Hash().Hex(), Phase: "used"}}, statuses)
		})
	}
}

func TestReceiptSweepRevisitsHistoryDuringAppends(t *testing.T) {
	for _, arrivals := range []int{1, 128} {
		t.Run(fmt.Sprintf("arrivals=%d", arrivals), func(t *testing.T) {
			f := newRecoveryRPC(t)
			chain, key := f.Chain(t), recoveryKey(t)
			s, _ := boundStore(t, "orchestrator", "chain", DefaultLimits)
			ticket, _ := winningTicket(t, 1)
			require.NoError(t, s.Admit(t.Context(), ticket))
			initial, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
			require.NoError(t, err)
			var previous *types.Transaction
			var hashes []ethcommon.Hash
			appendHistory := func(n int) {
				for range n {
					var next *types.Transaction
					parent := ""
					if previous == nil {
						next = new(types.Transaction)
						require.NoError(t, next.UnmarshalBinary(initial.Raw))
					} else {
						parent = previous.Hash().Hex()
						bump := func(n *big.Int) *big.Int {
							return new(big.Int).Add(new(big.Int).Div(new(big.Int).Mul(n, big.NewInt(112)), big.NewInt(100)), big.NewInt(1))
						}
						next, err = key.SignTxForChain(types.NewTx(&types.DynamicFeeTx{ChainID: previous.ChainId(), Nonce: previous.Nonce(), Gas: previous.Gas(), GasFeeCap: bump(previous.GasFeeCap()), GasTipCap: bump(previous.GasTipCap()), To: previous.To(), Data: previous.Data(), Value: previous.Value()}), big.NewInt(1))
						require.NoError(t, err)
					}
					identity, err := signedIdentity(next)
					require.NoError(t, err)
					require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, parent))
					hashes = append(hashes, next.Hash())
					previous = next
				}
			}
			appendHistory(129)
			var seen []ethcommon.Hash
			for pass := range 4 {
				ctx, cancel := context.WithCancel(t.Context())
				before := len(seen)
				reader := receiptReaderFunc(func(_ context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
					seen = append(seen, hash)
					if arrivals == 1 && pass == 0 {
						cancel()
						return nil, context.Canceled
					}
					return nil, nil
				})
				failures := ReconcileSubmitted(ctx, s, reader)
				if arrivals == 1 && pass == 0 {
					require.NotEmpty(t, failures)
				} else {
					require.Empty(t, failures)
				}
				cancel()
				require.LessOrEqual(t, len(seen)-before, 128)
				appendHistory(arrivals)
			}
			visits := 0
			for _, hash := range seen {
				if hash == hashes[0] {
					visits++
				}
			}
			require.Greater(t, visits, 1, "arrivals cannot keep a finite sweep from revisiting older identities")
			require.Equal(t, hashes[1], seen[1], "cancellation preserves the first unvisited row")
		})
	}
}

func TestRedeemerSQLiteFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payment.sqlite")
	store, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(path + suffix)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
	require.NoError(t, store.Close())
	require.NoError(t, os.Chmod(path, 0644))
	_, err = OpenRedeemerDB(path)
	require.ErrorContains(t, err, "owner-only")
}

func TestRedeemerMigrationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "redeemer.sqlite")
	store, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	defer store.Close()
	files, err := fs.Sub(MigrationFiles, "migrations")
	require.NoError(t, err)
	require.NoError(t, migrations.Down(t.Context(), store.db, files))
	require.NoError(t, store.Close())
	store, err = OpenRedeemerDB(path)
	require.NoError(t, err, "startup must reapply the rolled-back catalog")
	require.NoError(t, store.Close())
}

// Adapted from go-livepeer/common/db_test.go's winning-ticket store tests,
// by Elad Mallel and Nico Vergauwen (404d24a9455cd0997af1be6d18faa998702cce69).
func TestSQLiteWinningTicketLifecycle(t *testing.T) {
	store, _ := boundStore(t, "orchestrator", "chain", DefaultLimits)
	payer := ethcommon.HexToAddress("0x1234")
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: payer, Recipient: ethcommon.HexToAddress("0x5678"),
		FaceValue: big.NewInt(1234), WinProb: big.NewInt(2345), TicketNonce: 123,
		RecipientRandHash: ethcommon.HexToHash("0xabcd"), CreationRound: 9,
		CreationRoundBlockHash: ethcommon.HexToHash("0x9876"), ParamsExpirationBlock: big.NewInt(1337)},
		Sig: []byte{1, 2, 3}, RecipientRand: big.NewInt(4567)}
	count, err := store.WinningTicketCount(payer)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, store.StoreWinningTicket(ticket))
	require.Error(t, store.StoreWinningTicket(ticket))
	selected, err := store.SelectEarliestWinningTicket(payer, 9)
	require.NoError(t, err)
	require.Equal(t, ticket, selected)
	selected, err = store.SelectEarliestWinningTicket(payer, 10)
	require.NoError(t, err)
	require.Nil(t, selected)
	f := newRecoveryRPC(t)
	f.Used[ticket.Hash()] = true
	tickRecovery(t, store, f.Chain(t), recoveryKey(t))
	count, err = store.WinningTicketCount(payer)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, store.db.QueryRow("SELECT count(*) FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&count))
	require.Equal(t, 1, count, "settlement retains the ticket and its replay evidence")
}

func TestRedemptionMigrationHistoryAndRollback(t *testing.T) {
	f := newRecoveryRPC(t)
	s, ticket := f.store(t, 9)
	c, key := f.Chain(t), recoveryKey(t)
	files, err := fs.Sub(MigrationFiles, "migrations")
	require.NoError(t, err)
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	// Seed a genuine v1 database, then upgrade without changing its ticket identity.
	prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
	require.NoError(t, err)
	_, err = s.db.Exec(`INSERT INTO redemption_attempts(sig,attempted_at,phase,raw_transaction,redeemer_address,nonce) VALUES(?,?,'submitted',?,?,?)`, ticket.Sig, time.Now().UTC().Format(time.RFC3339Nano), prepared.Raw, prepared.From.Hex(), "0")
	require.NoError(t, err)
	_, err = s.db.Exec("UPDATE winning_tickets SET tx_hash=? WHERE sig=?", prepared.Hash.Hex(), ticket.Sig)
	require.NoError(t, err)
	backfillStart := time.Now().Add(-time.Second)
	require.NoError(t, migrations.Up(t.Context(), s.db, files))
	var enqueued string
	require.NoError(t, s.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&enqueued))
	enqueuedAt, err := time.Parse(time.RFC3339Nano, enqueued)
	require.NoError(t, err)
	require.WithinRange(t, enqueuedAt, backfillStart, time.Now())
	history, err := s.SubmittedRedemptions()
	require.NoError(t, err)
	require.Equal(t, []SubmittedRedemption{{Signature: ticket.Sig, Hash: prepared.Hash}}, history)
	var phase string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", prepared.Hash.Hex()).Scan(&phase))
	require.Equal(t, "submitted", phase)
	// A lossless rollback remains possible with the original representable state.
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Up(t.Context(), s.db, files))
	_, err = s.db.Exec("UPDATE redemption_attempts SET phase='retryable' WHERE sig=?", ticket.Sig)
	require.NoError(t, err)
	next := prepared
	next.Hash = ethcommon.HexToHash("0xabcdef")
	next.Nonce = 1
	require.NoError(t, s.RecordPrepared(t.Context(), ticket, next))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.NoError(t, migrations.Down(t.Context(), s.db, files))
	require.ErrorContains(t, migrations.Down(t.Context(), s.db, files), "cannot_downgrade_redemption_transaction_history")
	history, err = s.SubmittedRedemptions()
	require.NoError(t, err)
	require.Len(t, history, 2)
}

func TestRedemptionRollbackPreservesTransactionProgress(t *testing.T) {
	for _, phase := range []string{"submitted", "expired"} {
		t.Run(phase, func(t *testing.T) {
			f := newRecoveryRPC(t)
			s, ticket := f.store(t, 22)
			c, key := f.Chain(t), recoveryKey(t)
			prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
			require.NoError(t, err)
			require.NoError(t, s.RecordPrepared(t.Context(), ticket, prepared))
			_, err = s.db.Exec("UPDATE redemption_transactions SET phase='submitted',acknowledged=1")
			require.NoError(t, err)
			_, err = s.db.Exec("UPDATE redemption_attempts SET phase='submitted'")
			require.NoError(t, err)
			if phase == "expired" {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, phase))
			}
			floor, err := s.SavedNonceFloor(t.Context(), key.Address())
			require.NoError(t, err)
			require.EqualValues(t, 1, floor)
			files, err := fs.Sub(MigrationFiles, "migrations")
			require.NoError(t, err)
			require.NoError(t, migrations.Down(t.Context(), s.db, files))
			require.NoError(t, migrations.Down(t.Context(), s.db, files))
			require.NoError(t, migrations.Down(t.Context(), s.db, files))
			require.ErrorContains(t, migrations.Down(t.Context(), s.db, files), "cannot_downgrade_redemption_transaction_history")
			floor, err = s.SavedNonceFloor(t.Context(), key.Address())
			require.NoError(t, err)
			require.EqualValues(t, 1, floor, "protected rollback must leave history intact")
		})
	}
}
