package redemption

import (
	"math/big"
	"testing"

	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestBoundQueueMetricStates(t *testing.T) {
	for _, tc := range []struct{ phase, state string }{
		{"rejected", "rejected"}, {"retryable", "eligible"}, {"awaiting_finality", "awaiting_finality"}, {"reverted", "blocked"}, {"invalid", "blocked"},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			s, _ := boundStore(t, "orchestrator", "http://redeemer.example", DefaultLimits)
			ticket, _ := winningTicket(t, 1)
			require.NoError(t, s.Admit(t.Context(), ticket))
			s.snapshot = func() (eth.ChainSnapshot, bool) {
				return eth.ChainSnapshot{Block: big.NewInt(100), Round: big.NewInt(1), CurrentRound: big.NewInt(1), TicketValidityPeriod: big.NewInt(3)}, true
			}
			require.NoError(t, s.delivery(t.Context(), ticket, "submitted", 0))
			if tc.phase == "rejected" {
				require.NoError(t, s.delivery(t.Context(), ticket, tc.phase, 1))
			} else {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, tc.phase))
			}
			stats, err := s.redemptionStats(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, stats.tickets[tc.state])
			require.Positive(t, stats.oldestTickets[tc.state])
			if tc.state != "eligible" {
				require.Zero(t, stats.tickets["eligible"])
			}
		})
	}
}

func TestRedemptionMetricsDurableAndIdempotent(t *testing.T) {
	f, store, ticket, _, _ := newRecoveryTest(t, 14)
	store.metrics = observability.New("orchestrator")
	store.metrics.Snapshot(store.CollectMetrics)
	clock := eth.ChainSnapshot{Block: new(big.Int).Sub(ticket.ParamsExpirationBlock, big.NewInt(1)), Round: big.NewInt(ticket.CreationRound)}
	store.snapshot = func() (eth.ChainSnapshot, bool) { return clock, true }
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", map[string]string{"state": "waiting"}))
	var enqueued string
	require.NoError(t, store.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&enqueued))
	clock.Block = new(big.Int).Set(ticket.ParamsExpirationBlock)
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", map[string]string{"state": "eligible"}))
	_, err := store.db.Exec("UPDATE winning_tickets SET params_expiration_block='broken' WHERE sig=?", ticket.Sig)
	require.NoError(t, err)
	values := map[string]float64{}
	store.CollectMetrics(func(name string, value float64, _ observability.Labels) { values[name] = value })
	require.Equal(t, map[string]float64{"collector_healthy": 0}, values, "collection failure must expose health and omit unavailable statistics")
	_, err = store.db.Exec("UPDATE winning_tickets SET params_expiration_block=? WHERE sig=?", ticket.ParamsExpirationBlock.String(), ticket.Sig)
	require.NoError(t, err)
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "awaiting_finality"))
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "awaiting_finality"))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "awaiting_finality"}))
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "retryable"))
	require.NoError(t, store.setTicketPhase(t.Context(), ticket.Sig, "retryable"))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	_, err = store.db.Exec(`CREATE TRIGGER fail_transition BEFORE UPDATE OF phase ON redemption_attempts WHEN NEW.phase='awaiting_finality' BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
	require.NoError(t, err)
	require.Error(t, store.setTicketPhase(t.Context(), ticket.Sig, "awaiting_finality"))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "awaiting_finality"}))
	_, err = store.db.Exec(`DROP TRIGGER fail_transition`)
	require.NoError(t, err)
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "used", "", f.CollateralSnapshot()))
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "used", "", f.CollateralSnapshot()))
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "confirmed", "", f.CollateralSnapshot()))
	require.EqualValues(t, 1, test.MetricSum(t, store.metrics.Registry, "redemption_settlements_total", nil))
	require.Zero(t, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", nil))
	store = reopenRecoveryStore(t, store)
	var restored string
	require.NoError(t, store.db.QueryRow("SELECT enqueued_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&restored))
	require.Equal(t, enqueued, restored)
	require.NoError(t, store.Close())
	store.metrics = observability.New("orchestrator")
	store.metrics.Snapshot(store.CollectMetrics)
	require.Zero(t, test.MetricSum(t, store.metrics.Registry, "collector_healthy", nil))
	require.Zero(t, test.MetricSum(t, store.metrics.Registry, "redemption_queue_tickets", nil))
}
