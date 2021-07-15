func TestRedeemWinningTicket_IsUsedTicket(t *testing.T) {
	claimant, b, smgr, tm := localSenderMonitorFixture()
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
	sm := NewSenderMonitor(claimant, b, smgr, tm, ts)
	sm.Start()
	defer sm.Stop()
	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	// test error
	b.isUsedErr = errors.New("isUsed error")
	tx, err := sm.redeemWinningTicket(signedT)
	assert.Nil(tx)
	assert.EqualError(err, b.isUsedErr.Error())

	// test used
	b.isUsedErr = nil
	b.usedTickets[signedT.Hash()] = true
	tx, err = sm.redeemWinningTicket(signedT)
	assert.Nil(tx)
	assert.EqualError(err, errIsUsedTicket.Error())

	// test not used
	b.usedTickets[signedT.Hash()] = false
	tx, err = sm.redeemWinningTicket(signedT)
	assert.Nil(err)
	assert.NotNil(tx)
	assert.True(b.IsUsedTicket(signedT.Ticket))
}

func TestRedeemWinningTicket_CheckAvailableFundsAndFaceValue(t *testing.T) {
	assert := assert.New(t)

	cfg, b, smgr, tm := localSenderMonitorFixture()
	addr := ethcommon.BytesToAddress([]byte("foo"))
	smgr.info[addr] = &SenderInfo{
		Deposit:       big.NewInt(500),
		WithdrawRound: big.NewInt(0),
		Reserve: &ReserveInfo{
			FundsRemaining:        big.NewInt(1000),
			ClaimedInCurrentRound: big.NewInt(0),
		},
	}
	smgr.claimedReserve[addr] = big.NewInt(0)

	ts := newStubTicketStore()
	sm := NewSenderMonitor(cfg, b, smgr, tm, ts)

	signedT := defaultSignedTicket(addr, uint32(0))

	// Trigger availableFunds() error
	smgr.err = errors.New("GetSenderInfo() error")
	_, err := sm.redeemWinningTicket(signedT)
	assert.EqualError(err, smgr.err.Error())

	smgr.err = nil

	// Trigger SuggestGasPrice() error
	gasPriceErr := errors.New("SuggestGasPrice() error")
	cfg.SuggestGasPrice = func(ctx context.Context) (*big.Int, error) { return nil, gasPriceErr }
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	_, err = sm.redeemWinningTicket(signedT)
	assert.EqualError(err, gasPriceErr.Error())

	// Trigger SuggestGasPrice() timeout
	timeoutErr := errors.New("SuggestGasPrice() timeout")
	cfg.RPCTimeout = 20 * time.Millisecond
	cfg.SuggestGasPrice = func(ctx context.Context) (*big.Int, error) {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return nil, timeoutErr
		case <-timer.C:
		}
		return nil, errors.New("incorrect timeout error")
	}
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	_, err = sm.redeemWinningTicket(signedT)
	assert.EqualError(err, timeoutErr.Error())

	// Trigger insufficient funds to cover redeem tx cost error when availableFunds < txCost
	cfg.RedeemGas = 1
	cfg.SuggestGasPrice = func(ctx context.Context) (*big.Int, error) { return big.NewInt(1000000000), nil }
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	_, err = sm.redeemWinningTicket(signedT)
	assert.Contains(err.Error(), "insufficient sender funds")

	// Trigger insufficient funds to cover redeem tx cost error when availableFunds = txCost
	funds, err := sm.availableFunds(addr)
	require.Nil(t, err)
	cfg.RedeemGas = 1
	cfg.SuggestGasPrice = func(ctx context.Context) (*big.Int, error) { return funds, nil }
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	_, err = sm.redeemWinningTicket(signedT)
	assert.Contains(err.Error(), "insufficient sender funds")

	// Trigger insufficient face value to cover redeem tx cost error when face value < txCost
	txCost := new(big.Int).Sub(funds, big.NewInt(1))
	cfg.RedeemGas = 1
	cfg.SuggestGasPrice = func(ctx context.Context) (*big.Int, error) { return txCost, nil }
	badSignedT := defaultSignedTicket(addr, uint32(0))
	badSignedT.FaceValue = new(big.Int).Sub(txCost, big.NewInt(1))
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	_, err = sm.redeemWinningTicket(signedT)
	assert.Contains(err.Error(), "insufficient ticket face value")

	// Trigger insufficient face value to cover redeem tx cost error when face value = txCost
	badSignedT.FaceValue = txCost
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	_, err = sm.redeemWinningTicket(signedT)
	assert.Contains(err.Error(), "insufficient ticket face value")

	// Pass available funds and face value check when availableFunds > txCost and face value > txCost
	cfg.RedeemGas = 0
	cfg.SuggestGasPrice = func(ctx context.Context) (*big.Int, error) { return big.NewInt(0), nil }
	sm = NewSenderMonitor(cfg, b, smgr, tm, ts)
	tx, err := sm.redeemWinningTicket(signedT)
	assert.Nil(err)
	assert.NotNil(tx)
}

func TestRedeemWinningTicket_SingleTicket_RedeemError(t *testing.T) {
	cfg, b, smgr, tm := localSenderMonitorFixture()
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
	sm := NewSenderMonitor(cfg, b, smgr, tm, ts)
	sm.Start()
	defer sm.Stop()

	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	b.redeemShouldFail = true
	tx, err := sm.redeemWinningTicket(signedT)
	assert.EqualError(err, "stub broker redeem error")
	assert.Nil(tx)
	used, err := b.IsUsedTicket(signedT.Ticket)
	assert.NoError(err)
	assert.False(used)
}

func TestRedeemWinningTicket_SingleTicket_CheckTxError(t *testing.T) {
	cfg, b, smgr, tm := localSenderMonitorFixture()
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
	sm := NewSenderMonitor(cfg, b, smgr, tm, ts)
	sm.Start()
	defer sm.Stop()
	expErr := errCheckTx(errors.New("checktx error"))
	b.checkTxErr = expErr
	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	tx, err := sm.redeemWinningTicket(signedT)
	assert.Nil(tx)
	assert.IsType(expErr, err)
}

func TestRedeemWinningTicket_SingleTicket(t *testing.T) {
	cfg, b, smgr, tm := localSenderMonitorFixture()
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
	sm := NewSenderMonitor(cfg, b, smgr, tm, ts)
	sm.Start()
	defer sm.Stop()
	assert := assert.New(t)

	signedT := defaultSignedTicket(addr, uint32(0))

	tx, err := sm.redeemWinningTicket(signedT)
	assert.Nil(err)
	assert.NotNil(tx)

	ok, err := b.IsUsedTicket(signedT.Ticket)
	assert.Nil(err)
	assert.True(ok)
}
