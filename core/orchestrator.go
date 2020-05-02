package core

func (orch *orchestrator) isActive(addr ethcommon.Address) (bool, error) {
	filter := &common.DBOrchFilter{
		CurrentRound: orch.rm.LastInitializedRound(),
		Addresses:    []ethcommon.Address{addr},
	}
	orchs, err := orch.node.Database.SelectOrchs(filter)
	if err != nil {
		return false, err
	}

	return len(orchs) > 0, nil
}

