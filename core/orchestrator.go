package core

func (orch *orchestrator) isActive() (bool, error) {
	filter := &common.DBOrchFilter{
		CurrentRound: orch.rm.LastInitializedRound(),
		Addresses:    []ethcommon.Address{orch.Address()},
	}
	orchs, err := orch.node.Database.SelectOrchs(filter)
	if err != nil {
		return false, err
	}

	return len(orchs) > 0, nil
}

