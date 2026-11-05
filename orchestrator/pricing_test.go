package orchestrator

import (
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestFeedUpdatesOnlyNewSessionPricesAndFailsClosedWhenStale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRegistry("secret", "https://orch.example", time.Second, time.Minute)
		r.setRate(big.NewRat(3600, 1), time.Now().Add(time.Minute))
		require.NoError(t, r.AddStatic(StaticRunner{ID: "r", RunnerURL: "https://runner.example", App: "app", Capacity: 3, PriceInfo: priceInfo{Price: "1", Unit: "hour", Currency: "usd"}}))
		sid, _, _, _, err := r.Reserve("r")
		require.NoError(t, err)
		r.setRate(big.NewRat(7200, 1), time.Now().Add(time.Minute))
		old, _, err := r.PriceForSession("r", sid)
		require.NoError(t, err)
		require.Equal(t, "1", old.Price.String())
		current, _, err := r.PriceForRunner("r")
		require.NoError(t, err)
		require.Equal(t, "2", current.Price.String())
		time.Sleep(time.Minute)
		require.Empty(t, r.Discovery()[0].Runners)
		_, _, _, status, err := r.Reserve("r")
		require.Error(t, err)
		require.Equal(t, 503, status)
		_, _, err = r.PriceForSession("r", sid)
		require.NoError(t, err, "existing agreed prices must survive feed outages")
		r.SetWeiPerUSD(big.NewRat(10800, 1))
		require.Len(t, r.Discovery()[0].Runners, 1)
		require.Zero(t, test.MetricSum(t, r.metrics.Registry, "price_feed_updates_total", nil), "setting a rate is not a feed read")
	})
}
