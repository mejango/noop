# V2 audit remediation — 2026-09-11

This implements the findings in [the V2 audit](v2-strategy-data-audit-2026-09-11.md), starting from production-code commit `7864bc9`. Work was integrated in an isolated V2 checkout; the existing V3 worktree was not edited. The audit describes the old code. This document describes the resulting behavior. Historical rebuilds are deferred: the rollout preserves existing observations, finalized outcome labels and portfolio history. New tables/columns support future operation; explicit repair tools remain opt-in.

| Audit findings | Result | Principal implementation / regression coverage |
| --- | --- | --- |
| 1–2: final prices and margin | The final normalized order must still satisfy its rule, score, price, quantity, funds and fresh margin checks. Every maker retry repeats the checks and preserves the approved buy ceiling or sell floor. | `bot/trade-policy.js`, `bot/order-pricing.js`, confirmation and execution audit tests |
| 3: lost orders and fills | Each submitted request is journaled before sending. Fills, cumulative watermarks, remaining reservations, budget and action state commit transactionally. Missing orders and failed cancellations remain unresolved. | `bot/order-accounting.js`, `test/execution-audit.test.js` |
| 4: stale synthetic exits | Resting exits are checked against current intent, position and closeable quantity. Replacement waits for cancellation and cumulative fill reconciliation. | Actual resting-manager and execution tests |
| 5: broad patient exceptions | Hypothetical patient-fill economics substitute only for the relevant capture condition; other conditions remain binding. Explicit reviewer rejections cannot be overridden. | `bot/trade-policy.js`, confirmation and policy tests |
| 6: missing outcome observations | Entry candidates, held contracts and instruments with outstanding evaluation horizons remain in the observation universe. Incomplete outcomes wait through the quote window. Individual receipt timestamps distinguish asynchronously fetched quotes. | `bot/observations.js`, `bot/db.js`, observation and data-integrity tests |
| 7, 9–10: misleading accounting | Gross options cashflow, recorded settlement cashflow and bounded settlement estimates are separate. Unknown fees remain unknown. Raw balance changes are not called return or drawdown; current external holdings are not projected backward. | Shared `bot/economic-events.js`, dashboard P&L and compiled-route tests |
| 8: dead bot beside live dashboard | The bundled supervisor observes both children and completed-cycle heartbeats, terminates peers on failure and exits unsuccessfully for the service manager to restart. | `scripts/process-supervisor.js`, actual subprocess tests |
| 11: inadequate replacement protection | A roll requires sufficient later-dated puts at the same or higher strike for the quantity being retired. A tiny or materially lower-strike position cannot authorize a full roll. | Shared coverage helper and policy tests |
| 12: inconsistent rankings | Current market-best edge and historical market-best edge use the same statistic. Execution selection remains distinct and has deterministic economic tie-breaks. | `bot/option-market-quality.js`, market-quality tests |
| 13: mixed-expiry skew | Put/call skew uses the same expiry and comparable absolute delta, interpolating only within available call observations. Unmatched observations stay unknown. | Shared matched-skew helper and regressions |
| 14: stale advisor facts | Bot, dashboard advisor and research defaults share strategy facts/configuration. Spending capacity is distinct from protection already held. | `bot/strategy-facts.json`, dashboard strategy tests |
| 15: unavailable account reported flat | Invalid API responses fail explicitly. Missing account state and Greeks remain unavailable instead of becoming empty positions or zero exposure. | Dashboard API and snapshot tests |
| 16: research lookahead | Features use completed prior hours and timestamp-bounded inputs. Historical selection cannot choose a later quote from the same hour. | Correlation and backtest regressions |
| 17: unlimited unknown liquidity | Zero or missing quoted depth does not produce a simulated fill. Partial exits preserve the remainder and allocate costs without inventing terminal liquidity. | Actual backtest simulator tests |
| 18: stale baseline/report crash | The current-strategy baseline shares the production call score/defaults. Historical alternatives are named separately. Report metadata uses each table's real time column. | Correlation report smoke and backtest tests |
| 19: divergent rollups | Live writes and rebuilds use one atomic aggregate implementation. Open interest is a latest-per-instrument stock, IV is observation-weighted, and unknown data remains null. | `bot/hourly-rollups.js`, live-versus-rebuild native SQLite tests |
| 20: wrong option enum | Call-premium queries use `C` and the configured comparable DTE/delta cohort. | Bot and dashboard data tests |
| 21: copied test policy | Tests load production modules or actual selected production declarations. A mutation probe verifies that disabling a real gate breaks the tests. | `test/helpers/load-production.js`, `npm run test:mutation` |

Lifecycle projections now rebuild affected instruments incrementally. Candidate evidence retains the rule snapshot, shared policy version/configuration, quote source, receipt timestamp and known quote age. Historical rows are not retroactively assigned provenance that was never captured.

## Execution recovery

See [execution recovery](execution-recovery.md) for journaled submissions. Recovery reads venue evidence and writes reconciled local accounting; it never submits or cancels an order. Missing or conflicting evidence preserves the block. Pre-upgrade orders need their existing local receipts checked against venue order/trade history; a venue order with no proven strategy provenance is not silently adopted.

The bot must be stopped for operator recovery so its in-memory cycle budget cannot overwrite repaired state. Legacy recovery uses `node bot/reconcile-legacy-order.js --order ORDER_ID --bot-stopped`, with the same `DATA_DIR` and signing environment. It requires exact local receipts and complete venue evidence; it refuses unproved adoption. A restart alone does not clear unresolved submission evidence.

Receipt fields and pagination were checked against the official [V2 generated schemas](https://github.com/derivexyz/derive-py/blob/e662f36f6b1ab326e97e595f131a1fa5cf6376a8/derive_client/data_types/generated_models.py), pinned before the SDK’s V3 migration. A full final history page must stop at the declared page count because requesting an overflow page repeats the last page.

## Economic history

Automatic V2 trade-evidence collection begins at a durable account-specific boundary recorded before the upgraded process can trade. It resumes from that boundary after restart, on a bounded schedule with pagination progress and coverage evidence. It does not automatically import older history. Events retain decimal source values and raw venue records. For trade events, `amount` is contract quantity; `cashflow_usd` is signed premium; `fee_usd` and `realized_pnl_usd` are separate nullable venue facts. Transfer `amount` is a signed quantity of its stated currency. These records are separate from the strategy's order-intent and spend tables.

Actual settlement and capital-flow exports can be imported without inventing completeness:

```sh
node scripts/import-economic-events.js /explicit/path/noop.db /explicit/path/evidence.json
```

The JSON object must contain `schema_version: 1`, an `events` array, and `coverage` with `account_id`, `dataset` (`trades`, `settlements`, or `transfers`), ISO `from_timestamp`/`to_timestamp`, and boolean `complete`. Complete coverage requires an `evidence_reference` identifying an exhaustive venue export. Each event requires a durable `event_id`, matching account, event type, timestamp, currency, decimal `amount`, source and original `raw_json`; valuation fields may be null. Conflicting identities fail the transaction. An empty history is complete only with explicit exhaustive coverage evidence.

Returns and drawdowns remain unavailable until reconciled capital flows and suitable valuations support them. This release does not manufacture missing historical quotes, settlements, fees, transfer-time valuations or external-exposure settings.

## Derived-data repair and archives

Historical repair is not part of startup or this rollout. Any later proposal must identify the affected rows, show before/after results on a copy, quantify the reporting benefit, and include verification and rollback. Before deciding whether to apply a repair, compare its results on an explicit copy of a verified snapshot:

```sh
node scripts/archive-v2-data.js --db /explicit/path/noop.db --out /explicit/archive/noop-2026-09-11.db
node bot/backfill-hourly.js --db /explicit/copy/noop.db
node bot/repair-decision-outcomes.js --db /explicit/copy/noop.db
```

The archive tool uses SQLite's backup mechanism so committed WAL data is included, verifies the copy, and writes a checksum/provenance manifest. It refuses overwrite and never prunes raw source rows. Keep dated snapshots off the operational volume and verify a restore periodically. Schedule archives in the deployment's backup system; repository code alone does not establish an off-host backup.

Rebuilds use retained raw evidence and replace derived hourly buckets atomically. A range may be specified with `--from` and `--to` at UTC hour boundaries; it is inclusive/exclusive. Rebuilding a period whose raw evidence has already been removed cannot recover it. The explicit outcome repair reconsiders incomplete labels; it cannot recover observations that were never recorded. Normal processing of already-pending observations continues, but startup does not reopen finalized historical labels.

Raw evidence remains append-only in V2. Partitioning operational reads across archived databases would require a tested query/migration layer; deleting old rows before that exists would break outcome and research reproducibility. The archive manifest provides a restore baseline for that later storage migration.

## Validation and deployment boundary

With Node 20, install both locked dependency sets (`npm ci` and `npm --prefix dashboard ci`). The default `npm test` runs production-function trading tests, model/API tests, execution and policy regressions, native SQLite/rollup/observation tests, dashboard reporting tests and the backtest simulator suite. Dashboard production build, TypeScript/lint and a compiled P&L route smoke test are also checked. Tests use temporary data and mocked venue responses; no live order is submitted. The integrated verification passed 797 tests, plus the mutation probe and compiled-route smoke test.

Read-only Railway inspection confirmed the active deployment was `7864bc9`, using the bundled `Dockerfile`, one replica and an `ON_FAILURE` restart policy. SQLite and a read-only venue query agreed on both open zero-fill orders. One old local put limit was $7.60 while its venue maker order was $7.50; the stored reservation was conservative, and the upgrade does not require guessing a fill.

The pre-upgrade SQLite backup `/data/backups/v2-pre-audit-2026-09-11T19-16-23-605Z.db` passed integrity verification (2,778,013,696 bytes, SHA256 `9cdb608e6122036712de2abde4743219c06bcf22d50b763b69ab61ded04f30fc`). Its manifest is beside it. Same-volume backup is not off-host disaster recovery. Production rollout status and subsequent service checks are recorded separately from local tests. No live historical rebuild has been performed. Historical event completeness still depends on authoritative venue records, and no historical profitability claim follows from these software tests.
