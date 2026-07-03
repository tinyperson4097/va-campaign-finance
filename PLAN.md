# Plan: Bronze → Gold, Tech-Debt Cleanup, and the Finished MVP

Goal: a more transparent Virginia campaign finance resource than VPAP — not by out-designing
their site, but by letting anyone query the cleaned data directly: a simple Streamlit app with
a search bar that doubles as a SQL editor, plus manipulable result tables. Data-focused,
free-hosted, shareable.

**Two ground rules for this plan:**
1. **`functions/name_normalization.py` is off-limits.** The hard-coded rules (Dominion/Clean VA
   sets, candidate fixes) are the product of months of iteration and exist for 100% accuracy on
   this dataset. Nothing below refactors or "cleans up" normalization logic. The only changes
   allowed are *additive* (new hard-coded rules as new mismatches are found, via the existing
   `test_normalization_on_table.py` workflow).
2. **No elaborate UI.** The interface is: search/SQL input → interactive tables. Streamlit on
   Community Cloud (free, hosted, shareable) is the default choice.

---

## 1. Where things stand today

Two parallel stacks live in this repo:

- **The real pipeline (Python + GCP)** — actually works:
  - `getcsv.ipynb` (run in Colab) scrapes `apps.elections.virginia.gov/SBE_CSV/CF/` into GCS
    bucket `va-cf-local` under `raw_data/` (**bronze**).
  - `processors/ScheduleABCDFI_processor.py` and `processors/scheduleh_processor.py` parse the
    CSVs, apply normalization from `functions/name_normalization.py`, and load BigQuery tables
    `virginia_elections.campaign_finance` and `virginia_elections.schedule_h` (**silver-ish**).
  - `processors/amendment_processor.py` dedupes amended reports into `cf_clean` /
    `schedule_h_clean`.
  - `python_analysis_scripts/create_mapping_tables.py` builds `committee_mappings` and
    `name_variations`; the other analysis scripts (balance continuity, latest balances,
    unmatched contributions, local spending) query those tables.
- **The legacy Node/SQLite stack (`src/`)** — the original prototype: Selenium downloader,
  SQLite builder, regex "natural language" parser. The README's `npm` workflow is aspirational
  ("wishful UI that does not exist yet"). Superseded by the Python/BigQuery pipeline.

**In-flight work (branch `mapping-table`, uncommitted):** refactor of
`unmatched_contributions_analysis_optimized.py` to key lookups on `committee_name_normalized`
instead of the `name_variations` table, plus one-off debug logging (Young Dems / Clean VA /
NON-CC blocks) that needs stripping before landing. Also an additive normalization rule
(`CAT PORTERFIELD` → `CATHY PORTERFIELD`) — keep.

---

## 2. Bug and tech-debt inventory (specific)

### Correctness bugs
1. **CSV "cleaning" can corrupt data** — `processors/ScheduleABCDFI_processor.py:126-139`.
   `_fix_embedded_quotes_universal` strips *any* quote (including apostrophes) not adjacent to a
   comma/line boundary, so `O'Brien` → `OBrien` in the stored silver data.
   `_remove_commas_newlines_within_quoted_strings` (`re.sub(r'([,\n])(?!")', '', ...)`) deletes
   every comma/newline not followed by a `"` — it only behaves if *every* field in the file is
   quoted; on any unquoted file it silently merges rows and columns. These run on **all** files
   (lines 481–483, 542–544, 631–633). This is the most likely source of the "glitchy, poorly
   normalized" symptoms — bad rows enter silver *before* normalization ever sees them, so it
   looks like a normalization problem but isn't.
2. **KeyError in conflict reporting** — `python_analysis_scripts/create_mapping_tables.py:99`
   references `row['normalized_committee_name']`; the column is `committee_name_normalized`.
   Any duplicate committee code crashes the warning path.
3. **Stale normalized columns.** Because normalized values are computed at load time, tables
   loaded before a normalization improvement (like the new Porterfield rule) disagree with the
   current function until a reprocess. Not a code bug — but the plan needs a cheap, standard way
   to re-apply the *current* function without a full re-parse (see Phase 2).
4. **Amendment dedup is fuzzy + pandas.** `amendment_processor.py` pulls whole tables into
   pandas and uses `fuzzywuzzy` with a 30-day window — slow, memory-hungry, and a different
   fuzzy library than `rapidfuzz` used elsewhere. Keeping the latest amendment per
   (`committee_code`, `due_date`) is deterministic — one SQL window function.

### Structural debt
5. **Debug scaffolding in production scripts** — the uncommitted diff adds ~150 lines of
   McClellan/Young Dems debugging to `unmatched_contributions_analysis_optimized.py` (1,008
   lines and growing). Dead code too: `committee_variations`, `cleaned_names_cache`, unused
   `is_mcclellan`.
6. **Folder-specific CSV patches** — `_clean_embedded_quotes_2018_12` etc. hard-code literal
   strings from specific bad rows; currently commented out in favor of the universal (broken)
   fixers. Targeted, hard-coded fixes for known-bad files are fine (same philosophy as the
   normalization rules) — they just need to actually be wired in and tested.
7. **Repo hygiene** — 65 MB of `analysis_results/` outputs committed; output CSVs
   (`committee_name.csv`, `dom.csv`, two 0-byte files) at repo root; no
   `requirements.txt`/`pyproject.toml`; no tests; README mixes the real pipeline with the
   defunct npm workflow.
8. **No automation** — ingestion is a manually-run Colab notebook, so the data is stale; no
   scheduled refresh.

---

## 3. Guiding decisions

1. **Retire the Node/SQLite stack.** BigQuery is the database; Streamlit is the interface.
   Move `src/` to `legacy/` (it stays in git history).
2. **Raw stays raw.** Silver keeps source values verbatim alongside the normalized columns, so
   normalization updates never require re-scraping or re-parsing — just re-running the
   normalization pass (Phase 2).
3. **Normalization code is frozen except for additive rules.** The existing
   `test_normalization_on_table.py` compare-before-apply workflow stays the official way to
   vet new rules.
4. **Deterministic transforms move into SQL.** Amendment dedup, mapping-table generation, and
   the aggregate marts are windowed SQL — faster, testable, and no pandas memory ceiling.
   (Plain `.sql` files run by a small runner script is fine; dbt is optional and can come
   later if wanted — it's not required for the MVP.)
5. **Python keeps three jobs:** scraping SBE → GCS (bronze), parsing/loading GCS → BigQuery
   (silver, including the frozen normalization pass), and the investigative analyses that
   genuinely need row-by-row logic (`unmatched_contributions`).

---

## 4. The phases

### Phase 0 — Repo hygiene and landing the in-flight branch (small, do first)
- [ ] Strip the debug blocks from `unmatched_contributions_analysis_optimized.py`, keep the
      committee-mappings-lookup refactor and the Porterfield rule, commit, merge
      `mapping-table`.
- [ ] Fix the `create_mapping_tables.py:99` KeyError.
- [ ] Add `pyproject.toml` (or `requirements.txt`): pandas, google-cloud-bigquery,
      google-cloud-storage, pandas-gbq, rapidfuzz (drop fuzzywuzzy), streamlit.
- [ ] `.gitignore` outputs; move `analysis_results/` and root CSVs out of git
      (`git rm --cached`), keep a small `samples/` if useful.
- [ ] Move `src/` → `legacy/`; rewrite README to describe only the real pipeline.
- [ ] Convert `getcsv.ipynb` into `ingest/download_to_gcs.py` (plain script, runnable locally
      or as a scheduled job later).

### Phase 1 — Fix the parser so silver is trustworthy (root cause of "glitchy scripts")
- [ ] Replace the two universal regex fixers with real CSV parsing: `pd.read_csv` with
      `quotechar`, `doublequote=True`, and `on_bad_lines` set to a *callback that logs and
      quarantines* bad rows (written to a `quarantine/` prefix in GCS) instead of silently
      mangling the whole file.
- [ ] Re-enable the folder-specific fixes (2018_12, 2022_07, 2023_10, 2023_11) — hard-coded
      per-file patches are acceptable here; add a fixture test for each so they can't silently
      break.
- [ ] Add a **row-count reconciliation report**: rows in source file vs. loaded vs.
      quarantined, per file, written to a `load_audit` BigQuery table. This is how we know the
      cleanup worked.
- [ ] Re-run the full backfill into fresh tables (`campaign_finance_v2`), diff row counts and
      spot-check known entities (Dominion, Clean VA totals) against the old tables before
      swapping.

### Phase 2 — Cheap re-normalization (no changes to the function itself)
- [ ] Add `renormalize.py`: reads distinct raw names from silver, applies the *current*
      `normalize_name()`, and updates the normalized columns via a mapping-table JOIN in
      BigQuery (never a full re-parse). This makes future additive rules (the Porterfield
      pattern) a 5-minute operation instead of a full reprocess.
- [ ] Regenerate `committee_mappings` / `name_variations` after each re-normalization —
      port `create_mapping_tables.py`'s most-frequent-name-wins logic to SQL
      (`ROW_NUMBER()`), keeping identical output columns so downstream scripts don't change.
- [ ] Keep `test_normalization_on_table.py` as the gatekeeper for any new rule.

### Phase 3 — Gold layer (SQL transforms in a new `sql/` dir + small runner) — done, unverified
Implemented as `sql/00_setup` (shared `parse_va_date` UDF) through `sql/04_data_quality`,
run in order by `sql/run_gold.py --project-id <PROJECT>`. Written and syntax-checked
(`sqlglot`, dialect `bigquery`) but **not run against live BigQuery** — no `bq`/`gcloud`
auth on this machine (see item 6 below). Two notes for whoever runs it first:
- `fact_transaction` / `unmatched_contributions` assume `cf_clean` stores `transaction_date`
  as the same raw string format as the raw `campaign_finance` table (handled by the new
  `parse_va_date` UDF) — confirm this against the actual BigQuery schema.
- `local_election_costs` only covers cities/towns (`district_normal`); the counties
  rollup from the legacy `aggregate-local-financing.py` needs a `mapped_county` column
  that isn't produced anywhere in this codebase, so it was left out rather than guessed.

Tables in dataset `virginia_elections_gold`:
- **Dims:** `dim_committee` (code, normalized name, type, party, candidate, active years),
  `dim_candidate` (person-level rollup), `dim_entity` (donors/vendors), `dim_report`
  (metadata + amendment lineage + filing deadlines from `functions/filing_deadlines.py`).
- **Facts:** `fact_transaction` (schedules A–F, I unified, latest-amendment rows only —
  replaces `amendment_processor.py` with one window function), `fact_report_summary`
  (Schedule G/H totals).
- **Marts:** money raised/spent per candidate per cycle; top donors per candidate/committee;
  donor → recipient edges; local election cost summaries (replaces
  `scheduleh_analysis_cities/counties` + `aggregate-local-financing`).
- **Data-quality findings as published tables** (this is a transparency feature VPAP doesn't
  have): balance-continuity failures, suspicious ending balances, and the unmatched
  Schedule D ↔ Schedule A contributions from `unmatched_contributions_analysis_optimized.py`.

### Phase 4 — The Streamlit MVP (the product) — built, not yet deployed
Implemented as `app.py` (single file, ~200 lines) plus `.streamlit/secrets.toml.example`
documenting the expected secrets shape. Verified: syntax-checks clean, boots under
`streamlit run` with no exceptions and serves the no-credentials warning path (no GCP
creds on this machine to test the actual BigQuery queries or sample queries against real
data — do that before deploying). Not yet pushed to Streamlit Community Cloud.

One app, deliberately minimal, deployed free on Streamlit Community Cloud:
- **One input box** that accepts either:
  - plain text → treated as a search over `dim_candidate` / `dim_committee` / `dim_entity`
    normalized names, results shown as tables with drill-in (click a candidate → their
    transactions/totals), or
  - anything starting with `SELECT` → run as SQL against the gold dataset.
- **Tables via `st.dataframe`** — sortable/filterable out of the box, plus a CSV download
  button on every result.
- **A "sample queries" sidebar** — canned SQL (top Dominion recipients, unmatched
  contributions, race cost comparisons) that loads into the editor so people learn the schema
  by example.
- **Safety rails on the SQL path:** read-only service account scoped to the gold dataset only
  (stored in Streamlit secrets); dry-run first and reject if `total_bytes_processed` exceeds a
  cap; `maximum_bytes_billed` set on every job; 30 s timeout; row limit on display.
- **`last_updated` shown in the header.**

Optionally, also flip the gold dataset to **public read in BigQuery** — that alone delivers
"API/SQL searchable" for power users at zero infra cost (they query from their own GCP
projects), and a REST API can be added later only if someone actually asks for it.

### Phase 5 — Freshness
- [ ] A single monthly job (Cloud Run job + Cloud Scheduler, or even a cron on any machine):
      download new SBE folders → load silver → re-normalize → rebuild gold → write
      `last_updated`. SBE publishes monthly, so monthly is enough.
- [ ] First run doubles as the catch-up backfill (bronze appears to stop in late 2025; today
      is mid-2026).

---

## 5. What "finished MVP" means

Anyone with the Streamlit link can:
1. Type "youngkin" and get a table of matching candidates/committees with totals —
   **normalized so the totals are actually right** — and drill into transactions.
2. Type `SELECT donor, SUM(amount) ... GROUP BY 1 ORDER BY 2 DESC`, get a sortable table in
   seconds, and download it as CSV.
3. Load a sample script from the sidebar and modify it — learning the schema by example.
4. See data-quality findings VPAP doesn't publish (unmatched contributions, balance
   discrepancies) as first-class tables.
5. Trust freshness: monthly auto-update with a visible `last_updated`.

**Sequencing note:** Phases 0–1 are the highest-leverage work — the parser corruption
masquerades as normalization glitches, and fixing it protects the months invested in the
normalization rules. Phase 4 is small once gold exists (Streamlit + BigQuery is ~200 lines
for the app described above).

## 6. Open items to verify against GCP (blocked locally — no `bq`/`gcloud` auth on this machine)
- Confirm bronze completeness in `gs://va-cf-local/raw_data/` (all months through 2026?).
- Row counts of `campaign_finance` / `cf_clean` / `schedule_h`, and whether the 1999–2011
  old-format folders were ever loaded (README suggests `--folders-after 2015` was the norm).
- Whether `name_variations` / `committee_mappings` in BigQuery match the current
  `create_mapping_tables.py` output (they predate the mapping-table branch refactor).
