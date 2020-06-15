func TestRedeemerServer_QueueTicket(t *testing.T) {
	assert := assert.New(t)

	recipient := pm.RandAddress()
	eth := &eth.StubClient{}
	sm := newStubSenderMonitor()
	sm.maxFloat = big.NewInt(100)
	r := &Redeemer{
		recipient: recipient,
		eth:       eth,
		sm:        sm,
		quit:      make(chan struct{}),
	}

	ticket := &net.Ticket{
		Sender:        pm.RandAddress().Bytes(),
		RecipientRand: big.NewInt(1337).Bytes(),
		TicketParams: &net.TicketParams{
			Recipient:         recipient.Bytes(),
			FaceValue:         big.NewInt(100).Bytes(),
			WinProb:           big.NewInt(100).Bytes(),
			RecipientRandHash: pm.RandBytes(32),
			ExpirationBlock:   big.NewInt(100).Bytes(),
		},
		SenderParams: &net.TicketSenderParams{
			Sig:         pm.RandBytes(32),
			SenderNonce: 1,
		},
		ExpirationParams: &net.TicketExpirationParams{
			CreationRound:          100,
			CreationRoundBlockHash: pm.RandHash().Bytes(),
		},
	}

	sm.shouldFail = errors.New("QueueTicket error")
	res, err := r.QueueTicket(context.Background(), ticket)
	assert.Nil(res)
	assert.EqualError(err, baseRPCErr+sm.shouldFail.Error())
	sm.shouldFail = nil

	res, err = r.QueueTicket(context.Background(), ticket)
	assert.Nil(err)
	assert.Equal(res, &net.QueueTicketRes{})
	assert.Equal(sm.queued[0], pmTicket(ticket))

	// test wrong recipient
	ticket.TicketParams.Recipient = pm.RandAddress().Bytes()
	res, err = r.QueueTicket(context.Background(), ticket)
	assert.Nil(res)
	assert.EqualError(err, fmt.Sprintf("rpc error: code = PermissionDenied desc = invalid ticket recipient 0x%x, expected %v", ticket.TicketParams.Recipient, r.recipient.Hex()))
}

func TestRedeemerClient_QueueTicket_RPCErr(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	rpc := net.NewMockTicketRedeemerClient(ctrl)
	defer ctrl.Finish()
	rc := &RedeemerClient{
		rpc:     rpc,
		senders: make(map[ethcommon.Address]*remoteSender), quit: make(chan struct{}),
	}

	ticket := &net.Ticket{
		Sender:        pm.RandAddress().Bytes(),
		RecipientRand: big.NewInt(1337).Bytes(),
		TicketParams: &net.TicketParams{
			Recipient:         pm.RandAddress().Bytes(),
			FaceValue:         big.NewInt(100).Bytes(),
			WinProb:           big.NewInt(100).Bytes(),
			RecipientRandHash: pm.RandBytes(32),
			ExpirationBlock:   big.NewInt(100).Bytes(),
		},
		SenderParams: &net.TicketSenderParams{
			Sig:         pm.RandBytes(32),
			SenderNonce: 1,
		},
		ExpirationParams: &net.TicketExpirationParams{
			CreationRound:          100,
			CreationRoundBlockHash: pm.RandHash().Bytes(),
		},
	}

	rpc.EXPECT().QueueTicket(gomock.Any(), gomock.Any()).Return(nil, errors.New("QueueTicket error"))
	assert.EqualError(rc.QueueTicket(pmTicket(ticket)), "QueueTicket error")
}

func TestRedeemerClient_QueueTicket_Success(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	rpc := net.NewMockTicketRedeemerClient(ctrl)
	defer ctrl.Finish()
	rc := &RedeemerClient{
		rpc:     rpc,
		senders: make(map[ethcommon.Address]*remoteSender), quit: make(chan struct{}),
	}

	ticket := &net.Ticket{
		Sender:        pm.RandAddress().Bytes(),
		RecipientRand: big.NewInt(1337).Bytes(),
		TicketParams: &net.TicketParams{
			Recipient:         pm.RandAddress().Bytes(),
			FaceValue:         big.NewInt(100).Bytes(),
			WinProb:           big.NewInt(100).Bytes(),
			RecipientRandHash: pm.RandBytes(32),
			ExpirationBlock:   big.NewInt(100).Bytes(),
		},
		SenderParams: &net.TicketSenderParams{
			Sig:         pm.RandBytes(32),
			SenderNonce: 1,
		},
		ExpirationParams: &net.TicketExpirationParams{
			CreationRound:          100,
			CreationRoundBlockHash: pm.RandHash().Bytes(),
		},
	}

	rpc.EXPECT().QueueTicket(gomock.Any(), gomock.Any()).Return(nil, nil)
	err := rc.QueueTicket(pmTicket(ticket))
	assert.Nil(err)
}
