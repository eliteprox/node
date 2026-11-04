// Package test provides shared test helpers.
package test

import (
	"crypto/ecdsa"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// WriteKeystore creates an owner-only keystore and exact-byte password file. A nil
// private key generates a new account. Minimal scrypt parameters are only for tests.
func WriteKeystore(t testing.TB, private *ecdsa.PrivateKey) (keystorePath, passwordPath string) {
	t.Helper()
	if private == nil {
		var err error
		private, err = crypto.GenerateKey()
		require.NoError(t, err)
	}
	const password = "  test keystore password\r\n"
	data, err := keystore.EncryptKey(&keystore.Key{
		Id: uuid.New(), Address: crypto.PubkeyToAddress(private.PublicKey), PrivateKey: private,
	}, password, 2, 1)
	require.NoError(t, err)
	dir := t.TempDir()
	keystorePath, passwordPath = filepath.Join(dir, "keystore.json"), filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(keystorePath, data, 0600))
	require.NoError(t, os.WriteFile(passwordPath, []byte(password), 0600))
	return keystorePath, passwordPath
}

// WriteFixedKeystore preserves the scalar-one account used by transaction fixtures.
func WriteFixedKeystore(t testing.TB) (keystorePath, passwordPath string) {
	t.Helper()
	private, err := crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)
	return WriteKeystore(t, private)
}
