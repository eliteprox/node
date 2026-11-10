package orchestrator

import (
	"github.com/livepeer/node/redemption"
	"github.com/spf13/cobra"
)

func redemptionCommand() *cobra.Command { return redemption.Command("orchestrator", "payments.sqlite") }
