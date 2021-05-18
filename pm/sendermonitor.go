func (sm *LocalSenderMonitor) redeemWinningTicket(ticket *SignedTicket) (*types.Transaction, error) {
	availableFunds, err := sm.availableFunds(ticket.Sender)
	if err != nil {
		return nil, err
	}

	// Fail early if ticket is used
	used, err := sm.broker.IsUsedTicket(ticket.Ticket)
	if err != nil {
		if monitor.Enabled {
			monitor.TicketRedemptionError()
		}
		return nil, err
	}
	if used {
		if monitor.Enabled {
			monitor.TicketRedemptionError()
		}
		return nil, errIsUsedTicket
	}

	ctx, cancel := context.WithTimeout(context.Background(), sm.cfg.RPCTimeout)
	gasPrice, err := sm.cfg.SuggestGasPrice(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	cancel()

	// We only submit a redemption if availableFunds covers the redemption tx cost
	// Otherwise, we return an error so we can try the redemption later
	txCost := new(big.Int).Mul(big.NewInt(int64(sm.cfg.RedeemGas)), gasPrice)
	if availableFunds.Cmp(txCost) <= 0 {
		return nil, errors.New("insufficient sender funds for redeem tx cost")
	}
	if ticket.FaceValue.Cmp(txCost) <= 0 {
		return nil, errors.New("insufficient ticket face value for redeem tx cost")
	}

	// Subtract the ticket face value from the sender's current max float
	// This amount will be considered pending until the ticket redemption
	// transaction confirms on-chain
	sm.subFloat(ticket.Ticket.Sender, ticket.Ticket.FaceValue)

	defer func() {
		// Add the ticket face value back to the sender's current max float
		// This amount is no longer considered pending since the ticket
		// redemption transaction either confirmed on-chain or was not
		// submitted at all
		//
		// TODO(yondonfu): Should ultimately add back only the amount that
		// was actually successfully redeemed in order to take into account
		// the case where the ticket was not redeemd for its full face value
		// because the reserve was insufficient
		if err := sm.addFloat(ticket.Ticket.Sender, ticket.Ticket.FaceValue); err != nil {
			glog.Error(err)
		}
	}()

	// Assume that that this call will return immediately if there
	// is an error in transaction submission
	tx, err := sm.broker.RedeemWinningTicket(ticket.Ticket, ticket.Sig, ticket.RecipientRand)
	if err != nil {
		if monitor.Enabled {
			monitor.TicketRedemptionError()
		}
		return nil, err
	}

	// Wait for transaction to confirm
	if err := sm.broker.CheckTx(tx); err != nil {
		if monitor.Enabled {
			monitor.TicketRedemptionError()
		}
		return nil, err
	}

	if monitor.Enabled {
		// TODO(yondonfu): Handle case where < ticket.FaceValue is actually
		// redeemed i.e. if sender reserve cannot cover the full ticket.FaceValue
		monitor.ValueRedeemed(ticket.Ticket.FaceValue)
	}

	return tx, nil
}
