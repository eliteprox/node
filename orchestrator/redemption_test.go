package orchestrator

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	rpcfixture "github.com/livepeer/node/internal/redemptiontest"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
	"math/big"
	"path/filepath"
	"testing"
)

func newRecoveryTest(t *testing.T, id byte) (*rpcfixture.RedemptionRPC, *RedeemerDB, *pm.SignedTicket, eth.PaymentChain, *eth.Key) {
	t.Helper()
	f := rpcfixture.NewRedemptionRPC(t)
	s, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: common.HexToAddress("0x1234"), Recipient: common.HexToAddress("0x5678"), FaceValue: big.NewInt(1000), WinProb: big.NewInt(100), TicketNonce: 1, RecipientRandHash: crypto.Keccak256Hash([]byte{id}), CreationRound: 1, ParamsExpirationBlock: big.NewInt(10)}, Sig: []byte{id, 2, 3}, RecipientRand: big.NewInt(int64(id))}
	require.NoError(t, s.StoreWinningTicket(ticket))
	f.Mu.Lock()
	f.Tickets = append(f.Tickets, ticket)
	f.Mu.Unlock()
	return f, s, ticket, f.Chain(t), rpcfixture.RedemptionKey(t)
}
