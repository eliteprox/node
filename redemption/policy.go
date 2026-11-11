package redemption

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
)

// StandalonePolicy checks the actual sender, including retained transactions
// from a previous key. All rebroadcast paths use Contracts.Broadcast.
func StandalonePolicy(chain eth.PaymentChain, id *big.Int) func(context.Context, *types.Transaction) error {
	return func(ctx context.Context, tx *types.Transaction) error {
		if tx.ChainId().Cmp(id) != 0 {
			return errors.New("redemption transaction chain mismatch")
		}
		account, err := types.Sender(types.LatestSignerForChainID(id), tx)
		if err != nil {
			return errors.New("invalid redemption transaction sender")
		}
		return chain.CheckRedeemerWallet(ctx, account)
	}
}

func (s *RedeemerDB) Binding(ctx context.Context) (Binding, error) {
	var b Binding
	var controller string
	err := s.db.QueryRowContext(ctx, "SELECT chain_id,controller,role,destination FROM redemption_binding WHERE id=1").Scan(&b.ChainID, &controller, &b.Role, &b.Destination)
	if err != nil {
		return b, err
	}
	b.Controller = common.HexToAddress(controller)
	return b, nil
}
