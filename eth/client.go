package eth

func (c *client) setContracts(opts *bind.TransactOpts) error {
	c.setTransactOpts(*opts)

	controller, err := contracts.NewController(c.controllerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating Controller binding: %v", err)
		return err
	}

	c.controllerSess = &contracts.ControllerSession{
		Contract: controller,
	}

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

	c.livepeerTokenSess = &contracts.LivepeerTokenSession{
		Contract: token,
	}

	glog.V(common.SHORT).Infof("LivepeerToken: %v", c.tokenAddr.Hex())

	serviceRegistryAddr, err := c.GetContract(crypto.Keccak256Hash([]byte("ServiceRegistry")))
	if err != nil {
		glog.Errorf("Error getting ServiceRegistry address: %v", err)
		return err
	}

	c.serviceRegistryAddr = serviceRegistryAddr

	serviceRegistry, err := contracts.NewServiceRegistry(serviceRegistryAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating ServiceRegistry binding: %v", err)
		return err
	}

	c.serviceRegistrySess = &contracts.ServiceRegistrySession{
		Contract: serviceRegistry,
	}

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

	c.bondingManagerSess = &contracts.BondingManagerSession{
		Contract: bondingManager,
	}

	// for L1 contracts backwards-compatibility
	l1BondingManager, err := contracts.NewL1BondingManager(bondingManagerAddr, c.backend)
	if err != nil {
		glog.Errorf("Error creating L1BondingManager binding: %v", err)
		return err
	}

	c.l1BondingManagerSess = &contracts.L1BondingManagerSession{
		Contract: l1BondingManager,
	}

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

	c.ticketBrokerSess = &contracts.TicketBrokerSession{
		Contract: broker,
	}

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

	c.roundsManagerSess = &contracts.RoundsManagerSession{
		Contract: roundsManager,
	}

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

	// Client should never transact with the Minter directly so we don't include transact opts
	c.minterSess = &contracts.MinterSession{
		Contract: minter,
	}

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

	c.livepeerTokenFaucetSess = &contracts.LivepeerTokenFaucetSession{
		Contract: faucet,
	}

	glog.V(common.SHORT).Infof("LivepeerTokenFaucet: %v", c.faucetAddr.Hex())

	return nil
}

