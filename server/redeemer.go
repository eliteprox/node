func (r *Redeemer) QueueTicket(ctx context.Context, ticket *net.Ticket) (*net.QueueTicketRes, error) {
	if ticket.GetTicketParams() == nil || ticket.GetSenderParams() == nil || ticket.GetExpirationParams() == nil {
		return nil, status.Error(codes.InvalidArgument, "incomplete ticket")
	}
	t := pmTicket(ticket)
	if r.recipient != t.Recipient {
		return nil, status.Error(codes.PermissionDenied, fmt.Sprintf("invalid ticket recipient 0x%x, expected %v", ticket.TicketParams.Recipient, r.recipient.Hex()))
	}
	if err := r.sm.QueueTicket(t); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.Infof("Ticket queued sender=0x%x", ticket.Sender)

	return &net.QueueTicketRes{}, nil
}

func (r *RedeemerClient) QueueTicket(ticket *pm.SignedTicket) error {
	ctx, cancel := context.WithTimeout(context.Background(), GRPCTimeout)
	defer cancel()
	_, err := r.rpc.QueueTicket(ctx, protoTicket(ticket))
	return err
}

func pmTicket(ticket *net.Ticket) *pm.SignedTicket {
	return &pm.SignedTicket{
		Ticket: &pm.Ticket{
			Recipient:              ethcommon.BytesToAddress(ticket.TicketParams.Recipient),
			Sender:                 ethcommon.BytesToAddress(ticket.Sender),
			FaceValue:              new(big.Int).SetBytes(ticket.TicketParams.FaceValue),
			WinProb:                new(big.Int).SetBytes(ticket.TicketParams.WinProb),
			SenderNonce:            ticket.SenderParams.SenderNonce,
			RecipientRandHash:      ethcommon.BytesToHash(ticket.TicketParams.RecipientRandHash),
			CreationRound:          ticket.ExpirationParams.CreationRound,
			CreationRoundBlockHash: ethcommon.BytesToHash(ticket.ExpirationParams.CreationRoundBlockHash),
			ParamsExpirationBlock:  new(big.Int).SetBytes(ticket.TicketParams.ExpirationBlock),
		},
		RecipientRand: new(big.Int).SetBytes(ticket.RecipientRand),
		Sig:           ticket.SenderParams.Sig,
	}
}

func protoTicket(ticket *pm.SignedTicket) *net.Ticket {
	return &net.Ticket{
		Sender:        ticket.Sender.Bytes(),
		RecipientRand: ticket.RecipientRand.Bytes(),
		TicketParams: &net.TicketParams{
			Recipient:         ticket.Recipient.Bytes(),
			FaceValue:         ticket.FaceValue.Bytes(),
			WinProb:           ticket.WinProb.Bytes(),
			RecipientRandHash: ticket.RecipientRandHash.Bytes(),
			ExpirationBlock:   ticket.ParamsExpirationBlock.Bytes(),
		},
		SenderParams: &net.TicketSenderParams{
			SenderNonce: ticket.SenderNonce,
			Sig:         ticket.Sig,
		},
		ExpirationParams: &net.TicketExpirationParams{
			CreationRound:          ticket.CreationRound,
			CreationRoundBlockHash: ticket.CreationRoundBlockHash.Bytes(),
		},
	}
}
