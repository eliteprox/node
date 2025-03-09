package eth

func (c *client) setContracts(opts *bind.TransactOpts) error {
	c.setTransactOpts(*opts)

	controller, err := contracts.NewController(c.controllerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating Controller binding: %v", err)
		return err
	}
	c.controller = controller

	glog.V(common.SHORT).Infof("Controller: %v", c.controllerAddr.Hex())

	tokenAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("LivepeerToken")))
	if err != nil {
		glog.Errorf("Error getting LivepeerToken address: %v", err)
		return err
	}

	c.tokenAddr = tokenAddr

	token, err := contracts.NewLivepeerToken(tokenAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating LivpeerToken binding: %v", err)
		return err
	}
	c.livepeerToken = token

	glog.V(common.SHORT).Infof("LivepeerToken: %v", c.tokenAddr.Hex())

	if c.serviceRegistryAddr == (ethcommon.Address{}) {
		c.serviceRegistryAddr, err = c.GetContract(crypto.Keccak256Hash([]byte("ServiceRegistry")))
		if err != nil {
			glog.Errorf("Error getting ServiceRegistry address: %v", err)
			return err
		}
	}

	serviceRegistry, err := contracts.NewServiceRegistry(c.serviceRegistryAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating ServiceRegistry binding: %v", err)
		return err
	}
	c.serviceRegistry = serviceRegistry

	glog.V(common.SHORT).Infof("ServiceRegistry: %v", c.serviceRegistryAddr.Hex())

	bondingManagerAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("BondingManager")))
	if err != nil {
		glog.Errorf("Error getting BondingManager address: %v", err)
		return err
	}

	c.bondingManagerAddr = bondingManagerAddr

	bondingManager, err := contracts.NewBondingManager(bondingManagerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating BondingManager binding: %v", err)
		return err
	}
	c.bondingManager = bondingManager

	// for L1 contracts backwards-compatibility
	l1BondingManager, err := contracts.NewL1BondingManager(bondingManagerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating L1BondingManager binding: %v", err)
		return err
	}
	c.l1BondingManager = l1BondingManager

	glog.V(common.SHORT).Infof("BondingManager: %v", c.bondingManagerAddr.Hex())

	brokerAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("TicketBroker")))
	if err != nil {
		glog.Errorf("Error getting TicketBroker address: %v", err)
		return err
	}

	c.ticketBrokerAddr = brokerAddr

	broker, err := contracts.NewTicketBroker(brokerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating TicketBroker binding: %v", err)
		return err
	}
	c.ticketBroker = broker

	glog.V(common.SHORT).Infof("TicketBroker: %v", c.ticketBrokerAddr.Hex())

	roundsManagerAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("RoundsManager")))
	if err != nil {
		glog.Errorf("Error getting RoundsManager address: %v", err)
		return err
	}

	c.roundsManagerAddr = roundsManagerAddr

	roundsManager, err := contracts.NewRoundsManager(roundsManagerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating RoundsManager binding: %v", err)
		return err
	}
	c.roundsManager = roundsManager

	glog.V(common.SHORT).Infof("RoundsManager: %v", c.roundsManagerAddr.Hex())

	minterAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("Minter")))
	if err != nil {
		glog.Errorf("Error getting Minter address: %v", err)
		return err
	}

	c.minterAddr = minterAddr

	minter, err := contracts.NewMinter(minterAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating Minter binding: %v", err)
		return err
	}
	c.minter = minter

	glog.V(common.SHORT).Infof("Minter: %v", c.minterAddr.Hex())

	faucetAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("LivepeerTokenFaucet")))
	if err != nil {
		glog.Errorf("Error getting LivepeerTokenFaucet address: %v", err)
		return err
	}

	c.faucetAddr = faucetAddr

	faucet, err := contracts.NewLivepeerTokenFaucet(faucetAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating LivepeerTokenFaucet binding: %v", err)
		return err
	}
	c.livepeerTokenFaucet = faucet

	glog.V(common.SHORT).Infof("LivepeerTokenFaucet: %v", c.faucetAddr.Hex())

	livepeerGovernorAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("LivepeerGovernor")))
	if err != nil {
		glog.Errorf("Error getting Governor address: %v", err)
		return err
	}

	c.livepeerGovernorAddr = livepeerGovernorAddr

	governor, err := contracts.NewGovernor(livepeerGovernorAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating Governor binding: %v", err)
		return err
	}
	c.livepeerGovernor = governor

	glog.V(common.SHORT).Infof("LivepeerGovernor: %v", c.livepeerGovernorAddr.Hex())

	return nil
}

