package redemption

import (
	"fmt"
	"io"
	"math/big"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/stretchr/testify/require"
)

func TestRetryCommandEnforcesDeploymentAndWallet(t *testing.T) {
	for _, tc := range []struct {
		component, role string
		registered      bool
	}{{"orchestrator", "orchestrator", false}, {"redeemer", "redeemer", false}, {"orchestrator", "redeemer", false}, {"redeemer", "redeemer", true}, {"orchestrator", "", false}, {"redeemer", "", false}} {
		t.Run(fmt.Sprintf("%s/role=%s/registered=%t", tc.component, tc.role, tc.registered), func(t *testing.T) {
			f := newRecoveryRPC(t)
			chain := f.Chain(t)
			s, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "tickets.sqlite"))
			require.NoError(t, err)
			defer s.Close()
			if tc.role != "" {
				require.NoError(t, s.Bind(t.Context(), Binding{ChainID: "1", Controller: common.HexToAddress("0x1000"), Role: tc.role, Destination: "chain"}, DefaultLimits, chain))
			}
			ticket, _ := winningTicket(t, 1)
			require.NoError(t, s.Admit(t.Context(), ticket))
			key := recoveryKey(t)
			prepared, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
			require.NoError(t, err)
			require.NoError(t, s.RecordPrepared(t.Context(), ticket, prepared))
			f.Mu.Lock()
			f.Wallet, f.Registered = key.Address(), tc.registered
			f.Mu.Unlock()
			t.Setenv("LIVEPEER_"+strings.ToUpper(tc.component)+"_RPC_URL", f.Server.URL)
			cmd := Command(tc.component, "tickets.sqlite")
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"--config", "", "--data-dir", t.TempDir(), "--network", "test", "--redeemer-db", s.path, "--chain-id", "1", "--retry-transaction", prepared.Hash.Hex(), "--submit"})
			err = cmd.ExecuteContext(t.Context())
			if tc.component != tc.role {
				require.ErrorContains(t, err, "redemption database deployment or role mismatch")
			} else if tc.registered {
				require.ErrorIs(t, err, eth.ErrRedeemerIsOrchestrator)
			} else {
				require.NoError(t, err)
				require.Len(t, f.Transactions(), 1)
				return
			}
			require.Empty(t, f.Transactions())
		})
	}
}
