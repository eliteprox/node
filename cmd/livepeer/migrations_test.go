package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testBinaryMigrations(t *testing.T, install string) {
	t.Helper()
	for _, component := range []struct{ name, flag, config string }{
		{"orchestrator", "--redeemer-db", "RedeemerDB = 'custom/state.sqlite'"},
		{"redeemer", "--redeemer-db", "RedeemerDB = 'custom/state.sqlite'"},
		{"signer", "--kafka-outbox-db", "[Kafka]\nOutboxDB = 'custom/state.sqlite'"},
	} {
		t.Run(component.name+" migrations", func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "config.toml"), []byte("KeystoreFile = 'missing-key'\nKeystorePasswordFile = 'missing-password'\n"+component.config), 0600))
			path := filepath.Join(root, "custom", "state.sqlite")
			direct, err := exec.Command(filepath.Join(install, "livepeer-"+component.name),
				"migrate", "--data-dir", root, "up").CombinedOutput()
			require.NoError(t, err, string(direct))
			require.FileExists(t, path)
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			require.Len(t, entries, 2, "only config and the custom database directory are created")
			t.Setenv("LIVEPEER_"+strings.ToUpper(component.name)+"_"+strings.ToUpper(strings.ReplaceAll(strings.TrimPrefix(component.flag, "--"), "-", "_")), "missing.sqlite")
			forwarded, err := exec.Command(filepath.Join(install, "livepeer"),
				component.name, "migrate", "status", "--data-dir", root, component.flag, path).CombinedOutput()
			require.NoError(t, err, string(forwarded))
			require.Equal(t, string(direct), string(forwarded))
			require.Contains(t, string(forwarded), `"applied": true`)
		})
	}
}
