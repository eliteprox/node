# Running a standalone redeemer

A redeemer accepts winning tickets from orchestrators and submits them onchain.
Its wallet pays gas; each ticket's recipient receives the proceeds. The wallet
must not be an onchain orchestrator. Fund it with a small amount of ETH.

Configure the wallet's keystore, RPC connection, and persistent storage using
[the example configuration](../configs/redeemer/config.example.toml), then run:

```sh
livepeer redeemer --config /etc/livepeer/redeemer.toml
```

The redeemer listener defaults to `127.0.0.1:8941`; metrics, `/healthz`, and
`/readyz` default to `127.0.0.1:8942`. Provide TLS and endpoint authentication
through your deployment or a proxy. Use a separate persistent database for each
redeemer; see [configuration](configuration.md) and [recovery](payment-recovery.md).
`--config` takes a local TOML or JSON file path, as with other components.

## Connect an orchestrator

Add these settings to an onchain orchestrator's configuration, using its
registered address and the redeemer's reachable URL:

```toml
OrchestratorAddress = "0xYOUR_REGISTERED_ORCHESTRATOR"
RedeemerURL = "http://127.0.0.1:8941"
RedeemerGrants = ["127.0.0.1:8941"]
```

`RedeemerGrants` permits private destinations; public addresses need no grant.
Keep RPC, ticket, pricing, and database settings in the orchestrator config.
Put wallet credentials and redemption transaction options in the redeemer config
and omit them from the forwarding orchestrator config: the redeemer signs and
pays for transactions. Local redemption is the default.

The orchestrator sends winning tickets to the redeemer when the L1 chain reaches
the expiration block of the payment parameters it issued for those tickets.
During a redeemer outage, the orchestrator keeps tickets in its local queue and
retries delivery. It can keep serving requests until that queue fills.
The redeemer's successful response tells the orchestrator that its ticket is
stored; redemption happens separately onchain. See the
[HTTP API](http-api.md#redeemer-http) and [metrics](metrics.md) for details.

Inspect redemptions using the same redeemer configuration file:

```sh
livepeer redeemer redemptions --config /etc/livepeer/redeemer.toml
```
