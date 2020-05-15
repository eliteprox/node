func (sm *senderMonitor) redeemWinningTicket(ticket *SignedTicket) (err error) {
	maxFloat, e := sm.MaxFloat(ticket.Ticket.Sender)
	if e != nil {
		err = e
		return
	}

	// if max float is zero, there is no claimable reserve left or reserve is 0
	if maxFloat.Cmp(big.NewInt(0)) <= 0 {
		sm.QueueTicket(ticket)
		err = errors.Errorf("max float is zero")
		return
	}

	// If max float is insufficient to cover the ticket face value, queue
	// the ticket to be retried later
	if maxFloat.Cmp(ticket.Ticket.FaceValue) < 0 {
		sm.QueueTicket(ticket)
		err = fmt.Errorf("insufficient max float sender=%v faceValue=%v maxFloat=%v", ticket.Ticket.Sender.Hex(), ticket.Ticket.FaceValue, maxFloat)
		return
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
		e := sm.addFloat(ticket.Ticket.Sender, ticket.Ticket.FaceValue)
		if e != nil {
			err = e
		}
	}()

	// Assume that that this call will return immediately if there
	// is an error in transaction submission
	tx, e := sm.broker.RedeemWinningTicket(ticket.Ticket, ticket.Sig, ticket.RecipientRand)
	if e != nil {
		if monitor.Enabled {
			monitor.TicketRedemptionError(ticket.Ticket.Sender.String())
		}
		err = e
		return
	}

	// Wait for transaction to confirm
	if e := sm.broker.CheckTx(tx); e != nil {
		if monitor.Enabled {
			monitor.TicketRedemptionError(ticket.Ticket.Sender.String())
		}

		err = e
		return
	}

	if monitor.Enabled {
		// TODO(yondonfu): Handle case where < ticket.FaceValue is actually
		// redeemed i.e. if sender reserve cannot cover the full ticket.FaceValue
		monitor.ValueRedeemed(ticket.Ticket.Sender.String(), ticket.Ticket.FaceValue)
	}

	return
}
