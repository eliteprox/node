// Package redeemer runs the standalone winning-ticket redemption service.
package redeemer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/cmd/version"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/nodeconfig"
	"github.com/livepeer/node/nodeconfig/migrations"
	"github.com/livepeer/node/redemption"
	"github.com/spf13/cobra"
)

type Params struct {
	ConfigFile string `name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig" descr:"Configuration file; empty disables discovery"`
	nodeconfig.Settings
	Listen                    boa.HTTPBind    `default:"127.0.0.1:8941" descr:"Ticket submission listener"`
	MetricsListen             boa.HTTPBind    `default:"127.0.0.1:8942" descr:"Metrics and probe listener"`
	Account                   *common.Address `descr:"Dedicated redemption wallet; must not be an orchestrator"`
	RedeemerDB                string          `file:"optional" descr:"Winning-ticket SQLite database"`
	KeystoreFile              string          `basepath:"source" optional:"true" descr:"Encrypted geth redemption account JSON file"`
	KeystorePassword          *string         `secret:"true" optional:"true" descr:"Keystore decryption password"`
	KeystorePasswordFile      string          `basepath:"source" secretfor:"KeystorePassword" descr:"Owner-only file containing exact password bytes"`
	RPCURL                    *url.URL        `name:"rpc-url" secret:"true" optional:"true"`
	RPCURLFile                string          `basepath:"source" name:"rpc-url-file" secretfor:"RPCURL"`
	ChainID                   *uint64         `min:"1"`
	Controller                *common.Address `name:"controller-address"`
	RedeemerTxTimeout         time.Duration   `default:"5m" descr:"Transaction inclusion timeout"`
	RedeemerMaxTxReplacements int             `default:"1" min:"0" descr:"Maximum fee replacements per transaction"`
	RedeemerMaxFeePerGas      *uint256.Int    `descr:"Optional maximum fee in wei per gas"`
	MaxPendingTickets         int             `default:"100000" min:"1" descr:"Maximum unsettled winning tickets"`
	MaxTicketsPerRecipient    int             `default:"10000" min:"1" descr:"Maximum unsettled tickets per recipient"`
	PrintConfig               bool            `optional:"true" boa:"noconfig,noenv" descr:"Print audited redacted TOML configuration"`
}

func (p Params) Validate() error {
	if p.Network == "offchain" {
		return errors.New("redeemer requires an on-chain network")
	}
	if p.ChainID == nil || *p.ChainID == 0 || p.Controller == nil || *p.Controller == (common.Address{}) || p.RedeemerDB == "" {
		return errors.New("redeemer requires chain-id, controller-address and redeemer-db")
	}
	if p.KeystorePassword == nil && p.KeystorePasswordFile == "" {
		return errors.New("redeemer requires a keystore password or password file")
	}
	if !p.Listen.IsValid() || !p.MetricsListen.IsValid() {
		return errors.New("listen and metrics-listen require IP:port addresses")
	}
	if err := destination.ValidateURL(p.RPCURL); err != nil {
		return errors.New("redeemer requires a valid RPC URL")
	}
	if p.RedeemerTxTimeout <= 0 || p.RedeemerMaxTxReplacements < 0 || p.MaxPendingTickets <= 0 || p.MaxTicketsPerRecipient <= 0 {
		return errors.New("invalid redemption timeout, replacement count or queue capacity")
	}
	if p.RedeemerMaxFeePerGas != nil && p.RedeemerMaxFeePerGas.IsZero() {
		return errors.New("maximum redemption fee must be positive")
	}
	return nil
}

func Root(out, errOut io.Writer) *cobra.Command {
	p := new(Params)
	cmd := nodeconfig.Command("redeemer", nodeconfig.Mainnet, boa.Cmd[Params]{Use: "livepeer-redeemer", Short: "Standalone winning-ticket redeemer", Version: version.String(), Params: p, RejectUnknown: true, Args: cobra.NoArgs,
		PostConfigFuncCtx: func(ctx *boa.HookContext, p *Params, _ *cobra.Command, _ []string) error {
			if p.Network == nodeconfig.Mainnet {
				nodeconfig.Default(ctx, &p.ChainID, new(uint64(42161)))
				nodeconfig.Default(ctx, &p.Controller, new(nodeconfig.Controller))
			}
			nodeconfig.Default(ctx, &p.RedeemerDB, "tickets.sqlite")
			return nil
		}, RunFuncCtxE: func(ctx *boa.HookContext, p *Params, cmd *cobra.Command, _ []string) error {
			if p.PrintConfig {
				return toml.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{"Network": p.Network, "Listen": p.Listen.String(), "MetricsListen": p.MetricsListen.String(), "Account": p.Account, "ChainID": p.ChainID, "Controller": p.Controller, "RedeemerDB": p.RedeemerDB, "RedeemerTxTimeout": p.RedeemerTxTimeout.String(), "RedeemerMaxTxReplacements": p.RedeemerMaxTxReplacements, "RedeemerMaxFeePerGas": p.RedeemerMaxFeePerGas, "MaxPendingTickets": p.MaxPendingTickets, "MaxTicketsPerRecipient": p.MaxTicketsPerRecipient})
			}
			if err := p.Validate(); err != nil {
				return boa.NewUserInputError(err)
			}
			return Serve(cmd.Context(), *p, cmd.ErrOrStderr())
		}}, &p.ConfigFile).ToCobra()
	type storageParams struct {
		ConfigFile string `persistent:"true" name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig"`
		nodeconfig.PersistentSettings
		RedeemerDB string `persistent:"true" file:"optional" default:"tickets.sqlite"`
	}
	storage := new(storageParams)
	cmd.AddCommand(redemption.Command("redeemer", "tickets.sqlite"), migrations.Command(nodeconfig.Command("redeemer", nodeconfig.Mainnet, boa.Cmd[storageParams]{Params: storage}, &storage.ConfigFile), &storage.RedeemerDB, redemption.MigrationFiles, redemption.OpenDatabase))
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.InitDefaultCompletionCmd()
	return cmd
}

func Serve(parent context.Context, p Params, logOut io.Writer) (result error) {
	if err := p.Validate(); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(logOut, nil)).With("component", "redeemer")
	metrics := observability.New("redeemer")
	key, err := eth.OpenAccount(p.KeystoreDir(), p.KeystoreFile, p.KeystorePassword, p.KeystorePasswordFile, p.Account)
	if err != nil {
		return fmt.Errorf("open redemption wallet: %w", err)
	}
	rpc, err := eth.NewRPC(p.RPCURL, nil)
	if err != nil {
		return err
	}
	defer rpc.Close()
	rpc.SetObserver(metrics.RPC)
	id := new(big.Int).SetUint64(*p.ChainID)
	if err = rpc.CheckChainID(ctx, id); err != nil {
		return err
	}
	contracts, err := eth.NewContracts(rpc, *p.Controller)
	if err != nil {
		return err
	}
	if p.RedeemerMaxFeePerGas != nil {
		contracts.MaxFeePerGas = p.RedeemerMaxFeePerGas.ToBig()
	}
	contracts.CheckRedemptionCost = true
	chain := eth.PaymentChain{Contracts: contracts}
	if err = chain.CheckRedeemerWallet(ctx, key.Address()); err != nil {
		return fmt.Errorf("check dedicated redemption wallet: %w", err)
	}
	contracts.BeforeBroadcast = redemption.StandalonePolicy(chain, id)
	store, err := redemption.OpenRedeemerDB(p.RedeemerDB)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.Bind(ctx, redemption.Binding{ChainID: id.String(), Controller: *p.Controller, Role: "redeemer", Destination: "chain"}, redemption.Limits{Pending: p.MaxPendingTickets, PerRecipient: p.MaxTicketsPerRecipient}, chain); err != nil {
		return err
	}
	var status struct {
		sync.Mutex
		snapshot  eth.ChainSnapshot
		completed time.Time
		ready     bool
	}
	store.Configure(logger, metrics, func() (eth.ChainSnapshot, bool) {
		status.Lock()
		defer status.Unlock()
		return status.snapshot, status.ready && time.Since(status.completed) < time.Minute
	})
	metrics.Snapshot(store.CollectMetrics)
	dest, err := redemption.NewChainDestination(ctx, store, chain, key, id, redemption.RedemptionOptions{Timeout: p.RedeemerTxTimeout, MaxReplacements: p.RedeemerMaxTxReplacements})
	if err != nil {
		return err
	}
	worker, err := redemption.NewWorker(store, chain, dest)
	if err != nil {
		return err
	}
	apiListener, err := net.Listen("tcp", p.Listen.String())
	if err != nil {
		return err
	}
	defer apiListener.Close()
	metricListener, err := net.Listen("tcp", p.MetricsListen.String())
	if err != nil {
		return err
	}
	defer metricListener.Close()
	probe := http.NewServeMux()
	probe.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	probe.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		status.Lock()
		ready := status.ready && time.Since(status.completed) < time.Minute
		status.Unlock()
		if !ready {
			http.Error(w, "redemption dependencies unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	probe.Handle("GET /metrics", metrics.Handler())
	handler := redemption.Handler(store)
	servers := []*http.Server{
		{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { metrics.HTTP(w, r, handler.ServeHTTP) }), BaseContext: func(net.Listener) context.Context { return ctx }, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 1 << 16, MaxHeaderValueCount: 128},
		{Handler: probe, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 1 << 16, MaxHeaderValueCount: 128},
	}
	var workers sync.WaitGroup
	errs := make(chan error, 2)
	workers.Go(func() { errs <- servers[0].Serve(apiListener) })
	workers.Go(func() { errs <- servers[1].Serve(metricListener) })
	workers.Go(func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			tick, done := context.WithTimeout(eth.WithRPCWork(ctx, "background"), 30*time.Second)
			for _, err := range worker.Process(tick) {
				metrics.Add("redemption_errors_total", 1, observability.Labels{"error_kind": observability.ErrorKind(err)})
				logger.Error("redemption pass failed", "event", "redemption_pass", "error_kind", observability.ErrorKind(err))
			}
			done()
			// Exhausting a processing batch is healthy progress. Probe the
			// dependencies with their own budget rather than its expired context.
			tick, done = context.WithTimeout(eth.WithRPCWork(ctx, "background"), 15*time.Second)
			snapshot, chainErr := chain.RedemptionSnapshot(tick, "latest")
			walletErr := chain.CheckRedeemerWallet(tick, key.Address())
			storageErr := store.CheckStorage(tick)
			available, capacityErr := store.CapacityAvailable(tick, common.Address{})
			if capacityErr != nil {
				storageErr = capacityErr
			} else if !available {
				storageErr = redemption.ErrCapacity
			}
			balance, balanceErr := rpc.BalanceAt(tick, key.Address(), nil)
			funded := balanceErr == nil && balance.Sign() > 0
			if balanceErr == nil {
				metrics.Gauge("redeemer_gas_balance_eth", nil).Set(observability.ETH(new(big.Rat).SetInt(balance)))
			}
			status.Lock()
			status.completed = time.Now()
			status.ready = chainErr == nil && walletErr == nil && storageErr == nil && funded
			if chainErr == nil {
				status.snapshot = snapshot
			}
			status.Unlock()
			metrics.Gauge("redemption_worker_last_completed_timestamp_seconds", nil).Set(float64(time.Now().Unix()))
			done()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	logger.Info("redeemer started", "event", "started", "listen", p.Listen, "metrics_listen", p.MetricsListen)
	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	cancel()
	shutdown, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	for _, server := range servers {
		if err = server.Shutdown(shutdown); err != nil {
			result = errors.Join(result, err)
			_ = server.Close()
		}
	}
	workers.Wait()
	return result
}
