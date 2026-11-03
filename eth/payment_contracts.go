package eth

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
)

// Address retention adapts livepeer/go-livepeer eth/client.go setContracts at
// 597f882cb9f4d42ad8430b0f592afd5929af4a6b (Nico Vergauwen, 829cd9047c46aef03ff5bf07b3e34be810694b74;
// Rafał Leszko, 3209dd946bb0376308044055b6caf262fd25cf61). Confirmation before
// dependent reads adapts Mike Zupper's BlueClawNetwork/livepeer-node
// eth/chainview.go, 2d95d40b8aa4699c7ca40838b6a9c312704c93db.
// Copyright (c) 2017 Livepeer; MIT notice in LICENSE.
type paymentContracts struct{ rounds, broker, bonding common.Address }

func (c *Contracts) paymentContractsAt(ctx context.Context, ref any) (paymentContracts, error) {
	var addresses paymentContracts
	for _, entry := range []struct {
		name string
		to   *common.Address
	}{{"roundsManager", &addresses.rounds}, {"ticketBroker", &addresses.broker}, {"bondingManager", &addresses.bonding}} {
		address, err := c.ResolveAt(ctx, ref, entry.name)
		if err != nil {
			return paymentContracts{}, err
		}
		*entry.to = address
	}
	return addresses, nil
}

func (c *Contracts) paymentAddress(ctx context.Context, s ChainSnapshot, name string) (common.Address, error) {
	if s.observer == nil {
		return c.ResolveAt(ctx, s.blockReference, name)
	}
	if s.observer.chain.Contracts != c {
		return common.Address{}, errors.New("payment snapshot belongs to another contract client")
	}
	switch name {
	case "ticketBroker":
		return s.addresses.broker, nil
	case "bondingManager":
		return s.addresses.bonding, nil
	default:
		return common.Address{}, errors.New("unsupported payment contract")
	}
}
