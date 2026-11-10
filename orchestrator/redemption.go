package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/redemption"
)

// RedeemerDB keeps the payment admission adapter alongside the shared queue.
type RedeemerDB struct {
	*redemption.RedeemerDB
	db        *sql.DB
	logger    *slog.Logger
	metrics   *observability.Metrics
	readiness *paidReadiness
}

var redeemerMigrationFiles = redemption.MigrationFiles
var openRedeemerDB = redemption.OpenDatabase

type RedemptionOptions = redemption.RedemptionOptions
type RedemptionStatus = redemption.RedemptionStatus

func OpenRedeemerDB(path string) (*RedeemerDB, error) {
	db, err := redemption.OpenDatabase(context.Background(), path, true)
	if err != nil {
		return nil, err
	}
	shared, err := redemption.NewStore(db)
	if err != nil {
		return nil, err
	}
	s := &RedeemerDB{RedeemerDB: shared, db: db}
	s.configure()
	return s, nil
}

func (s *RedeemerDB) configure() {
	s.RedeemerDB.Configure(s.logger, s.metrics, func() (eth.ChainSnapshot, bool) {
		p := s.readiness
		if p == nil {
			return eth.ChainSnapshot{}, false
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.snapshot, time.Since(p.completed) <= max(time.Minute, 3*p.interval) && p.chain && p.snapshot.Validate() == nil
	})
}

func (s *RedeemerDB) collectMetrics(emit observability.Emit) { s.RedeemerDB.CollectMetrics(emit) }
func (s *RedeemerDB) checkStorage(ctx context.Context) error { return s.CheckStorage(ctx) }
func (s *RedeemerDB) storeWinningTickets(ctx context.Context, payer common.Address, randHash common.Hash, minRound int64, collateral *big.Int, tickets []*pm.SignedTicket, snapshot pm.ChainSnapshot) error {
	err := s.StoreWinningTickets(ctx, payer, randHash, minRound, collateral, tickets, snapshot)
	if errors.Is(err, redemption.ErrExposedRandomness) {
		return ErrInvalidPayment
	}
	if errors.Is(err, eth.ErrObservationUnavailable) {
		return paymentObservation{err, "error"}
	}
	return err
}

func (s *RedeemerDB) shared() *redemption.RedeemerDB {
	if s == nil {
		return nil
	}
	return s.RedeemerDB
}

func StartRedemptionRecovery(ctx context.Context, s *RedeemerDB, c eth.PaymentChain, k *eth.Key, o RedemptionOptions) error {
	return redemption.StartRedemptionRecovery(ctx, s.shared(), c, k, o)
}
func ProcessRedemptions(ctx context.Context, s *RedeemerDB, c eth.PaymentChain, k *eth.Key, id *big.Int) []error {
	return redemption.ProcessRedemptions(ctx, s.shared(), c, k, id)
}
func RetryRedemption(ctx context.Context, s *RedeemerDB, c *eth.Contracts, h common.Hash) error {
	return redemption.RetryRedemption(ctx, s.shared(), c, h)
}

// Retain the orchestrator's exported queue entry points for callers upgrading
// alongside the shared implementation.
type ReceiptReader = redemption.ReceiptReader
type SubmittedRedemption = redemption.SubmittedRedemption

func ReconcileSubmitted(ctx context.Context, s *RedeemerDB, c ReceiptReader) []error {
	return redemption.ReconcileSubmitted(ctx, s.shared(), c)
}
func RedeemPending(ctx context.Context, s *RedeemerDB, c eth.PaymentChain, k *eth.Key, id *big.Int, snapshot eth.ChainSnapshot) []error {
	return redemption.RedeemPending(ctx, s.shared(), c, k, id, snapshot)
}
