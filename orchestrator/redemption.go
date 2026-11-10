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

func (s *RedeemerDB) storeWinningTickets(ctx context.Context, payer common.Address, randHash common.Hash, collateral *big.Int, tickets []*pm.SignedTicket, snapshot pm.ChainSnapshot) error {
	err := s.StoreWinningTickets(ctx, payer, randHash, collateral, tickets, snapshot)
	if errors.Is(err, redemption.ErrExposedRandomness) {
		return ErrInvalidPayment
	}
	if errors.Is(err, redemption.ErrCapacity) {
		return errPaymentTicketCapacity
	}
	if errors.Is(err, eth.ErrObservationUnavailable) {
		return paymentObservation{err, "error"}
	}
	return err
}
