func TestRedeemWinningTicket_SingleTicket_ZeroMaxFloat(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(500),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}

	ts := newStubTicketStore()
	smgr.claimedReserve[addr] = big.NewInt(100)
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()

	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.EqualError(err, "max float is zero")
}

func TestRedeemWinningTicket_SingleTicket_RedeemError(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(1000),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}

	ts := newStubTicketStore()
	smgr.claimedReserve[addr] = big.NewInt(100)
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()

	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	b.redeemShouldFail = true
	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.EqualError(err, "stub broker redeem error")

	used, err := b.IsUsedTicket(signedT.Ticket)
	assert.NoError(err)
	assert.False(used)
}

func TestRedeemWinningTicket_SingleTicket_CheckTxError(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(1000),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}

	ts := newStubTicketStore()
	smgr.claimedReserve[addr] = big.NewInt(100)
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()
	b.checkTxErr = errors.New("checktx error")
	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.EqualError(err, b.checkTxErr.Error())
}

func TestRedeemWinningTicket_SingleTicket(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(1000),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}

	ts := newStubTicketStore()
	smgr.claimedReserve[addr] = big.NewInt(100)
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()
	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.Nil(err)

	ok, err := b.IsUsedTicket(signedT.Ticket)
	assert.Nil(err)
	assert.True(ok)
}

func TestRedeemWinningTicket_MaxFloatError(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()

	ts := newStubTicketStore()
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()

	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))
	smgr.err = errors.New("maxfloat err")
	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.EqualError(err, smgr.err.Error())
}

func TestRedeemWinningTicket_InsufficientMaxFloat_QueueTicket(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(525),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}

	ts := newStubTicketStore()
	smgr.claimedReserve[addr] = big.NewInt(100)
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()

	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.EqualError(err, fmt.Sprintf("insufficient max float sender=%v faceValue=%v maxFloat=%v", addr.Hex(), signedT.FaceValue, new(big.Int).Sub(new(big.Int).Div(smgr.info[addr].Reserve.FundsRemaining, tm.transcoderPoolSize), smgr.claimedReserve[addr])))
}

func TestRedeemWinningTicket_AddFloatError(t *testing.T) {
	claimant, b, smgr, tm := senderMonitorFixture()
	addr := RandAddress()
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(5000),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}

	ts := newStubTicketStore()
	smgr.claimedReserve[addr] = big.NewInt(100)
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts, 5*time.Minute, 3600)
	sm.Start()
	defer sm.Stop()

	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))
	sm.(*senderMonitor).ensureCache(addr)
	sm.(*senderMonitor).senders[addr].pendingAmount = big.NewInt(-100)

	err := sm.(*senderMonitor).redeemWinningTicket(signedT)
	assert.EqualError(err, "cannot subtract from insufficient pendingAmount")
}
