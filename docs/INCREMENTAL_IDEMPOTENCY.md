# Incremental Processing and Idempotency

## Purpose

This contract defines how FinStream handles repeated ingestion, immutable Bronze
runs, PostgreSQL replay, source revisions, incremental retrieval, and stable
analytical results. It distinguishes execution provenance from source business
identity so that reruns are safe without discarding legitimate provider
corrections.

## Core Semantics

- **Source record identity** is the dataset-specific identity of a represented
  provider record or occurrence. It does not include `run_id`.
- **Ingestion run identity** is `(source, dataset, run_id)`. `run_id` is the
  UTC `ingested_at` instant formatted to microsecond precision and is execution
  provenance, not source business identity.
- **Bronze artifact identity** is `source + dataset + run_id + filename`; the
  fixed filenames are `payload.json` and `data.parquet`.
- **PostgreSQL replay idempotency** means replaying the same already committed
  ingestion run is a verified no-op: it neither adds nor changes committed
  source rows.
- **Incremental ingestion** narrows or reconciles future retrievals using
  source-aware boundaries while retaining revision semantics.
- **End-to-end idempotency** means that repeated successful execution does not
  create additional logical analytical records or change analytical meaning
  solely because it was rerun. Immutable Bronze provenance may differ across
  distinct runs.

A different `run_id` may legitimately contain the same source business identity
because providers can revise, correct, or later reissue data. Cross-run history
is therefore append-oriented.

## Dataset Identity and Incremental Strategy

| Dataset | Logical identity / analytical grain | Cross-run behavior | Incremental strategy |
| --- | --- | --- | --- |
| Twelve Data `daily_market_prices` | `(symbol, trading_date)` | Repeated identities are retained; Silver selects the latest representation. | Automatic ingestion uses the per-symbol PostgreSQL `max(trading_date)` minus a three-calendar-day overlap; no history keeps the unbounded bootstrap fetch, and explicit ranges take precedence. |
| FRED `series_metadata` | `series_id` identifies a mutable metadata snapshot. | New runs may retain another snapshot for the series. | Full metadata fetch remains appropriate; returned timestamps are change signals, not request cursors. |
| FRED `series_observations` | `(series_id, observation_date, realtime_start, realtime_end)` | Repeated contexts are retained; Silver selects the latest representation and Gold selects the latest context per series/date. | Automatic ingestion uses the per-series PostgreSQL `max(observation_date)` minus a 365-calendar-day overlap; no history keeps full retrieval, while explicit observation bounds take precedence. Omitted real-time bounds retain FRED's current-vintage defaults. |
| SEC EDGAR `submissions` | `(cik, accession_number)` | Repeated filings may be retained across runs. | Full/current refresh of the implemented `filings.recent` source scope; the project does not follow supplemental `filings.files` responses. No date or accession watermark is safe for corrections or removals. |
| SEC EDGAR `company_facts` | Current analytical deduplication grain: `(cik, taxonomy, concept, unit, start_date, end_date, accession_number, fiscal_year, fiscal_period, form, filed_date, frame)` | Flattened occurrences are retained; Silver selects the latest representation. | Full current Company Facts response for each requested CIK; no bounded request parameter or PostgreSQL watermark is used. |

The Company Facts grain is an analytical deduplication grain, not a proven
globally complete XBRL fact identity. The source-aligned schema lacks an XBRL
context identifier and dimensional qualifiers, so the represented tuple cannot
prove uniqueness for every possible XBRL occurrence.

SEC refreshes do not treat an accession number, filing date, fact end date, or
ingestion timestamp as a source-change cursor. A later current response is
written as a new immutable run so corrected representations remain available.
The current Company Facts Silver model resolves repeated represented rows, but
it is not snapshot-aware: without a recorded complete-snapshot boundary per
CIK, a source disappearance cannot safely invalidate an older represented row.
The submissions scope is only `filings.recent`, so it likewise cannot establish
historical absence or removal. No tombstones or removal inference are produced.

## Bronze Semantics

Bronze artifacts are stored under:

```text
<root>/<source>/<dataset>/ingestion_date=YYYY-MM-DD/run_id=<run_id>/
    payload.json
    data.parquet
```

- Artifacts are immutable and existing files are never overwritten.
- Different `run_id` values may contain the same logical source records.
- `run_id` records execution provenance rather than source business identity.
- Retrying the identical Bronze run verifies or reconstructs its canonical
  artifacts when their captured Raw JSON proves the same-run state.
- Raw JSON is written before Parquet, so a partial failure can leave a raw-only
  run.

## Same-Run Recovery

- A retry with the same `run_id` first inspects only its canonical
  `payload.json` and `data.parquet` paths.
- With no artifacts, normal provider retrieval proceeds. With valid Raw JSON
  and missing Parquet, FinStream reparses the captured payload and recreates
  only the missing Parquet artifact without a provider request.
- With a complete pair, the stored Parquet must exactly match deterministic
  reconstruction from Raw JSON; a verified pair is reused without overwrite or
  refetch.
- Parquet-only, malformed Raw JSON, invalid/mismatched Parquet, or unexpected
  run-directory contents fail deterministically and require operator action.
- Normal PostgreSQL loaders retain fail-fast replay verification. A committed
  registry/source mismatch and a mixed FRED or SEC combined state are not
  automatically repaired; callers must resolve them explicitly.

## PostgreSQL Replay Contract

`source_data.ingestion_runs` identifies a loaded dataset by
`(source, dataset, run_id)`.

- A first load completes source-specific validation, registers the ingestion
  run, inserts source rows, and returns the loader's existing count or combined
  result.
- An exact replay uses the same ingestion-run identity. Conflict-aware registry
  registration uses `ON CONFLICT (source, dataset, run_id) DO NOTHING` and then
  verifies the committed state.
- Replay verification requires exact equality of `source`, `dataset`, `run_id`,
  `ingested_at`, `raw_json_path`, `parquet_path`, and `record_count`, plus an
  exact count of source rows for that ingestion run.
- A verified replay performs no insert, update, or delete against existing
  source state. Database-owned `loaded_at` is neither compared nor changed.
- Zero-row replay is valid when the registry metadata matches and the source
  row count is zero.
- Metadata or row-count mismatch fails clearly. The loader does not repair,
  overwrite, delete, backfill, or mutate historical state.
- A different `run_id` remains a normal append-oriented load, even if it has
  the same business identity. No global cross-run business-key uniqueness is
  imposed.
- Transactions are caller-owned: loaders do not commit, roll back, or close
  connections.

The contract applies to Twelve Data daily prices, FRED metadata and
observations, and SEC submissions and Company Facts. FRED and SEC combined
loads require their related datasets to be consistently absent or consistently
verified. A mixed or inconsistent combined state fails rather than loading only
one half.

## Transformation Semantics

Current Silver and Gold dbt models are views, not dbt incremental models.
Silver resolves repeated cross-run source representations with its
latest-representation rules. Repeated builds against unchanged source state are
therefore analytically stable. Source-specific incrementality does not require
dbt incremental materialization under the current model grains and recency
rules.

## Risks and Limitations

- Company Facts lacks a proven globally complete XBRL occurrence identity.
- Providers can revise values under the same logical identity; global
  insert-ignore behavior would lose revisions.
- Append-only snapshots alone do not represent source removals or withdrawals.
- SEC refreshes preserve corrections through distinct runs, but current source
  scope and run metadata cannot safely interpret an absent submission or fact
  as a deletion.
- FRED `observation_date` is neither a revision nor publication watermark.
- Maximum-date watermarks do not prove gap-free completeness; explicit Market
  date ranges and full or explicit FRED refetches remain manual backfill paths.
- The finite FRED overlap can capture recent revisions but cannot detect deep
  historical revisions outside the requested observation window.
- A zero-row ingestion run cannot currently be attributed from the registry
  alone to a requested symbol, CIK, or FRED series.
- Valid raw-only Bronze states can reconstruct their missing Parquet artifact;
  malformed or inconsistent artifact states require operator action.
- Transaction recovery remains caller-owned.

Any future reconciliation logic must preserve the immutable-run and cross-run
revision contracts above.

## Non-goals

This contract does not imply:

- global cross-run business-key deduplication;
- checkpoint or scheduler-state tables;
- complete FRED vintage history;
- complete SEC historical filing coverage;
- automatic repair of inconsistent historical state; or
- dbt incremental materialization.
