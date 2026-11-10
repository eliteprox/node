package main

import (
	"fmt"
	"os"

	"github.com/livepeer/node/redeemer"
)

func main() {
	if err := redeemer.Root(os.Stdout, os.Stderr).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
