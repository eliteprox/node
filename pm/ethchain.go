package pm

import (
	"context"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
)

type ChainSnapshot = eth.ChainSnapshot

// PayerChain contains the payer collateral observation used by both payment sides.
type PayerChain interface {
	PayerFunds(context.Context, ethcommon.Address, ethcommon.Address) (PayerFunds, error)
}

// PaymentChain contains only the Ethereum reads needed by payment receipt.
type PaymentChain interface {
	Snapshot(context.Context) (ChainSnapshot, error)
	IsActiveAt(context.Context, ethcommon.Address, ChainSnapshot) (bool, error)
	PayerChain
}

type PayerFunds = eth.SenderInfo

var _ PaymentChain = eth.PaymentChain{}
