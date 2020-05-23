func (sm *LocalSenderMonitor) redeemWinningTicket(ticket *SignedTicket) error {
	maxFloat, err := sm.MaxFloat(ticket.Ticket.Sender)
	if err != nil {
		return err
	}

	// if max float is zero, there is no claimable reserve left or reserve is 0
	if maxFloat.Cmp(big.NewInt(0)) <= 0 {
		if err := sm.QueueTicket(ticket); err != nil {
			return err
		}
		return errors.New("max float is 0")
	}

	// If max float is insufficient to cover the ticket face value, queue
	// the ticket to be retried later
	if maxFloat.Cmp(ticket.Ticket.FaceValue) < 0 {
		if err := sm.QueueTicket(ticket); err != nil {
			return err
		}
		return fmt.Errorf("insufficient max float sender=%v faceValue=%v maxFloat=%v", ticket.Ticket.Sender.Hex(), ticket.Ticket.FaceValue, maxFloat)
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
			monitor.TicketRedemptionError(ticket.Ticket.Sender.String())
		}
		return err
	}

	// Wait for transaction to confirm
	if err := sm.broker.CheckTx(tx); err != nil {
		if monitor.Enabled {
			monitor.TicketRedemptionError(ticket.Ticket.Sender.String())
		}
		return err
	}

	if monitor.Enabled {
		// TODO(yondonfu): Handle case where < ticket.FaceValue is actually
		// redeemed i.e. if sender reserve cannot cover the full ticket.FaceValue
		monitor.ValueRedeemed(ticket.Ticket.Sender.String(), ticket.Ticket.FaceValue)
	}

	return nil
}
