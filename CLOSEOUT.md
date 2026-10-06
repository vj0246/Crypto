# Cascade Clock: Close-out Plan

Assumption: "contain and end" means define a finish line and stop there, not keep
expanding scope. Pick one end state, do only its checklist, then archive.

## Where the project stands

Built and tested (15 tests): schema, config, Parquet/DuckDB storage, margin and
liquidation-price math, Hyperliquid client, and `fuel/liquidation.py` (liquidation
inference from the trade stream).

Not built: recorder entrypoint, position poller, fuel map, `hawkes/`, `pricing/`,
`eval/`, README. Nothing has run against live data.

## Step 1: Choose the end state (only you can decide)

| Option | Scope | Effort | Verdict |
|---|---|---|---|
| A. Shelve | Stop now. Library plus decision log. | None | Fine if the idea is no longer worth pursuing. |
| B. Data pipeline (recommended) | Record trades, infer liquidations, build the fuel map, validate on live data. Stop. | Small | A real, finished deliverable with a clear end. |
| C. Full model | B plus Hawkes fit, COS option pricing, evaluation. | Open-ended | Only if you have a testable hypothesis and time. |

Write your choice at the top of `decisions.md` as `D7`.

## Step 2: Actions only you can do

1. Run the code from a network that can reach `api.hyperliquid.xyz`. The cloud sandbox gets 403.
2. Confirm against one live payload each:
   - trade message: `users` is `[buyer, seller]`, `side` is the taker side
   - `clearinghouseState` uses the field name `liquidationPx`
3. Find the liquidator counterparty address(es) in live data and set `CASCADE_LIQUIDATOR_ADDRESSES`.
4. Hand-label a small live sample of liquidations (even 20 to 30) to get a real precision number. Without it, accuracy stays unmeasured.
5. Decide whether to open a PR into `main` or just keep the branch. No PR exists yet.

## Step 3: Remaining engineering for option B (small)

1. Recorder entrypoint: wire `stream_trades` to `LiquidationDetector(sink=ParquetWriter.append)`.
2. Position poller: seed the detector from `clearinghouse_state`. Note the budget:
   `BUDGET_PER_MIN=1000` and weight 2 allows about 500 polls per minute, so the default
   `fuel_universe_size=1500` takes about 3 minutes per cycle. It conflicts with
   `fuel_poll_interval_s=60`. Lower the universe or lengthen the interval.
3. Fuel map: compute `FUEL_SCHEMA` rows (notional at risk per price band) from tracked positions.
4. Run it for 24 hours, then review `stats()` (tracked count, A/B agreement rate) and the
   labelled sample from Step 2.4.
5. Tune `liq_price_tolerance` and `liq_cross_check_min_dev` on that data.

## Step 4: Definition of done (option B)

- [ ] Live payload assumptions confirmed or corrected in code
- [ ] 24h recording ran without crashing, files readable via `cascade.store.db.connect()`
- [ ] Precision measured on the labelled sample, number written in `decisions.md`
- [ ] README with how to run and known limits
- [ ] Tests pass: `python -m pytest`

## Step 5: Shutdown checklist

- [ ] Merge or discard branch `claude/task-o6cwfj`; tag the final commit (e.g. `v0.1-closeout`)
- [ ] Stop any recorder processes and scheduled routines
- [ ] Delete or archive `data/` (gitignored, so it is not in the repo)
- [ ] Secrets: none are required for the free-data path. Confirm no `.env` was committed
      (only `.env.example` is tracked)
- [ ] Archive the GitHub repo if you are finished with it

## Stop rules (to keep it contained)

- No new modules beyond the chosen option's list.
- If measured precision on the labelled sample is poor and tuning does not fix it, stop
  and record that as the project's finding. Do not build the Hawkes model on weak events.
