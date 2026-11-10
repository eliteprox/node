# Redemption source history

The shared queue preserves Node's ticket and transaction records. Its upstream
source is pinned to go-livepeer
`afaaa36e743d39e6f460e7993d516d52b3a81341`.

The scoped source history is retained as an additional parent using the pattern
from [Node PR #40](https://github.com/livepeer/node/pull/40). It preserves original
authors, committers, dates, and messages, with `Original-Commit` and
`Source-Scope` trailers. The filtered ancestry contains 37 effective changes;
its tip is `ff9c88a9354ec0b0333b1e685ba4389c43b16415`.

| Upstream source | Treatment |
| --- | --- |
| `pm/queue.go`, `pm/queue_test.go` | Queue history retained; adapted to bounded durable scheduling |
| `pm/sendermonitor.go` | History scoped to `redeemWinningTicket`; reused redemption and face-value/cost policy |
| `pm/sendermonitor_test.go` | Winning-ticket redemption tests retained, excluding max-float mutation tests |
| `server/redeemer.go` | History scoped to both `QueueTicket` methods and ticket conversion; HTTP adaptation attributed in source |
| `server/redeemer_test.go` | QueueTicket and conversion test history retained |
| `net/redeemer.proto` and generated gRPC bindings | Excluded; Node uses HTTP |
| Max-float calculation, subscriptions, and client balance cache | Excluded |
| `doc/redeemer.md` | Operating guide rewritten for Node |

Earlier source attributions in Node remain valid. In particular, the existing
queue/recovery port cites `0ff08af1`, ticket types cite `bd645a09`, and SQLite
operations cite `404d24a9`. Retaining the new scoped ancestry does not replace
those notices. The original MIT notice is in [LICENSE](../LICENSE).
