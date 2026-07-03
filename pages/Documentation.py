#!/usr/bin/env python3
"""
Live schema + pipeline reference for the gold dataset.

Column names/types come from BigQuery's INFORMATION_SCHEMA at page-load time,
so renaming/adding/dropping a column in a sql/*.sql file and rerunning
sql/run_gold.py is all it takes to keep this page correct -- nothing here is
hand-maintained. Table descriptions are the header comment already written at
the top of each sql/*.sql file (single source of truth, not a second copy to
forget to update). Column-level definitions and normalization provenance
below are hand-authored (COLUMN_DEFINITIONS / NORMALIZATION_INFO) since those
can't be derived from BigQuery or the SQL text -- they degrade gracefully
("no definition yet") for any column not listed, so a new column never
breaks the page, it's just undocumented until someone adds a line here.
"""

import re
from pathlib import Path

import streamlit as st

from bq_client import AUTH_HELP, DefaultCredentialsError, GOLD_DATASET, PROJECT_ID, get_bigquery_client, require_project_id

st.set_page_config(page_title="Documentation - VA Campaign Finance", layout="wide")

SQL_ROOT = Path(__file__).parent.parent / "sql"

# Acronyms spelled out in the human-readable page. Deliberately NOT applied to
# the "Copy for an LLM" block -- that text is for a chat AI, which doesn't
# need VA/SQL/etc. spelled out, and a novice pasting it in isn't the one
# reading it directly.
ACRONYMS = {
    "SBE": "State Board of Elections",
    "VA": "Virginia",
    "GCS": "Google Cloud Storage",
    "CC": "Candidate Committee",
    "SQL": "Structured Query Language",
    "CSV": "Comma-Separated Values",
    "PAC": "Political Action Committee",
}


def expand_acronyms(text: str) -> str:
    """Expand each acronym's first standalone occurrence in `text` (plural-aware)."""
    for acronym, full in ACRONYMS.items():
        pattern = re.compile(rf"\b{re.escape(acronym)}(s?)\b")
        text = pattern.sub(lambda m: f"{acronym}{m.group(1)} ({full})", text, count=1)
    return text


# Hand-authored, one entry per column name (shared across tables where the
# same name means the same thing). Missing entries render as "no definition
# yet" rather than erroring, so new columns never break this page.
COLUMN_DEFINITIONS = {
    "report_id": "Unique ID for one filed campaign finance report (one committee's disclosure for one filing period).",
    "committee_code": "Virginia SBE's unique ID for a committee (e.g. one candidate's committee for one cycle).",
    "committee_name": "Committee name as filed on the report, unmodified.",
    "committee_name_raw": "Most common raw (non-normalized) committee name seen for this committee_code.",
    "committee_name_normalized": "Standardized committee name -- use this to group/join, not the raw name, since the same committee is spelled inconsistently across filings. See Normalization.",
    "candidate_name": "Candidate name as filed on the report, unmodified. Blank for non-candidate committees (PACs, party committees).",
    "candidate_name_normalized": "Standardized candidate name -- use this to group a candidate's committees/cycles together. See Normalization.",
    "committee_codes": "Every committee_code this candidate has filed under, across cycles.",
    "is_candidate_committee": "TRUE if this committee represents a single candidate; FALSE for PACs, party committees, and other non-candidate committees.",
    "committee_type": "Committee category as filed, e.g. 'Candidate Campaign Committee', 'Political Action Committee', 'Political Party Committee', 'Out of State Political Committee'.",
    "party": "Political party as filed.",
    "first_seen_year": "Earliest report_year this committee/entity appears in the data.",
    "last_seen_year": "Most recent report_year this committee/entity appears in the data.",
    "office_sought": "Office sought as filed on the report, unmodified (e.g. 'Delegate - 42nd', 'HOD').",
    "office_sought_normal": "Office sought, standardized to a fixed set of categories (delegate, senator, governor, mayor, city council, school board, etc.).",
    "district": "District as filed on the report, unmodified.",
    "district_normal": "District, standardized: numeric districts have leading zeros stripped; local races are prefixed with the city/town name (e.g. 'Richmond (2)'); at-large and mayoral races are district '0'.",
    "level": "Government level: 'federal', 'state', or 'local', derived from office_sought_normal and district.",
    "candidate_city": "City the candidate is running in/from, as filed.",
    "election_cycle": "Election date this report's activity counts toward, as filed (e.g. '11/2023').",
    "most_recent_election_cycle": "The election_cycle from this candidate's most recent report.",
    "primary_or_general": "'primary' or 'general', derived from election_cycle (a November election_cycle is 'general', everything else is 'primary').",
    "first_election_cycle_year": "Earliest report_year this candidate appears in the data.",
    "last_election_cycle_year": "Most recent report_year this candidate appears in the data.",
    "report_year": "Calendar year the report covers.",
    "report_date": "Date the report was filed.",
    "due_date": "Date the report was due.",
    "amendment_count": "How many times this report has been amended; 0 for an original filing.",
    "current_amendment_count": "amendment_count for the more recent of the two reports being compared.",
    "is_amendment": "TRUE if amendment_count > 0.",
    "is_latest_amendment": "TRUE if this is the most-amended version of the report for its (committee_code, due_date) -- filter to TRUE to avoid double-counting superseded amendments.",
    "current_due_date": "due_date of the later of two consecutive reports being compared for balance continuity.",
    "previous_due_date": "due_date of the earlier of the two reports.",
    "current_report_date": "report_date of the later of two consecutive reports.",
    "previous_report_date": "report_date of the earlier of the two reports.",
    "data_source": "'old' (pre-2012 SBE format) or 'new' (2012+ format) -- which raw CSV schema this row was parsed from.",
    "folder_name": "The SBE source folder this row came from (e.g. '2023_10' for new-format data, a bare year like '1999' for old-format data).",
    "schedule_type": "Which SBE schedule this transaction came from (Schedule A-I) -- see transaction_category for what each means.",
    "transaction_type": "Same as schedule_type on most rows; kept as its own column because it's what the raw source data calls it.",
    "transaction_category": "'receipt' (Schedule A/B/C: itemized, in-kind, and other money received) or 'disbursement' (Schedule D/F/I: itemized, debts/obligations, and in-kind money spent).",
    "transaction_date": "Date of the individual transaction (not the report date).",
    "amount": "Dollar amount of the transaction.",
    "total_to_date": "Running total for this entity/committee as reported on the filing, if provided.",
    "total_raised": "Sum of receipt-category transaction amounts.",
    "total_spent": "Sum of disbursement-category transaction amounts.",
    "receipt_count": "Number of receipt-category transactions.",
    "disbursement_count": "Number of disbursement-category transactions.",
    "total_amount": "Sum of transaction amounts for this grouping (donor/candidate/edge, depending on the table).",
    "contribution_count": "Number of contributions in this grouping.",
    "transaction_count": "Number of transactions represented by this edge.",
    "first_contribution_date": "Earliest transaction_date in this grouping.",
    "last_contribution_date": "Most recent transaction_date in this grouping.",
    "entity_name": "Donor/vendor name as filed on the transaction, unmodified.",
    "entity_name_normalized": "Standardized donor/vendor name -- use this to group the same donor across spelling variants. See Normalization.",
    "entity_first_name": "Donor/vendor first name, if the source row had one (individuals).",
    "entity_last_name": "Donor/vendor last name or company name, as filed.",
    "entity_address": "Donor/vendor street address as filed.",
    "entity_city": "Donor/vendor city as filed.",
    "entity_state": "Donor/vendor state as filed.",
    "entity_zip": "Donor/vendor ZIP code as filed.",
    "entity_employer": "Donor's employer, if provided (individual contributions only).",
    "entity_occupation": "Donor's occupation, if provided (individual contributions only).",
    "entity_is_individual": "TRUE if the donor/vendor is a person, FALSE if an organization, per the source filing's flag.",
    "is_individual": "TRUE if this entity was ever flagged as a person on any transaction.",
    "name_variations": "Every raw spelling seen for this entity_name_normalized, e.g. all the ways 'Dominion Energy' has appeared across filings.",
    "purpose": "Stated reason/description for the transaction (item purchased, service rendered, or purpose of the obligation).",
    "zip_code": "Filing committee's ZIP code, as reported.",
    "submitted_date": "Date the report was submitted (may differ from report_date).",
    "onTime": "TRUE if the transaction's report_date was on or before the applicable filing deadline; NULL if it couldn't be determined.",
    "starting_balance": "Cash balance at the start of the reporting period (Schedule H).",
    "ending_balance": "Cash balance at the end of the reporting period (Schedule H).",
    "line_19": "'Expendable Funds Balance', separately reported on Schedule H line 19 -- should agree with ending_balance; see suspicious_ending_balances for cases where it doesn't.",
    "total_disbursements": "Total money spent this reporting period (Schedule H).",
    "current_starting_balance": "starting_balance of the later of two consecutive reports being compared.",
    "previous_ending_balance": "ending_balance of the earlier of two consecutive reports being compared.",
    "current_ending_balance": "ending_balance of the later of two consecutive reports being compared.",
    "balance_discrepancy": "current_starting_balance minus previous_ending_balance -- nonzero means the committee's own numbers don't connect between consecutive reports.",
    "line_19_discrepancy": "Absolute difference between ending_balance and line_19 on the same report.",
    "negative_ending_balance": "TRUE if ending_balance is less than 0.",
    "line_19_mismatch": "TRUE if ending_balance and line_19 disagree by more than a cent.",
    "max_disbursements": "Highest total_disbursements among candidates in this race/cycle.",
    "avg_disbursements": "Average total_disbursements among candidates in this race/cycle.",
    "num_candidates": "Number of distinct candidates in this race/cycle.",
    "source_name": "Where the money came from in this edge (a donor's normalized name for receipts, a committee's normalized name for disbursements).",
    "target_name": "Where the money went in this edge (a committee's normalized name for receipts, a vendor/payee's normalized name for disbursements).",
    "edge_type": "'receipt' or 'disbursement' -- which direction this edge represents.",
    "donor_committee_code": "committee_code of the committee that made the (Schedule D) expenditure being checked for a match.",
    "donor_committee_name": "Name of the committee that made the expenditure, as filed.",
    "donor_committee_name_normalized": "Standardized name of the committee that made the expenditure.",
    "recipient_name": "Name the expenditure was made out to, as filed.",
    "recipient_name_normalized": "Standardized name the expenditure was made out to.",
    "matched_committee_code": "committee_code this recipient_name_normalized was matched to via committee_mappings.",
    "matched_committee_name_normalized": "Standardized name of the matched recipient committee.",
    "matched_candidate_name": "Candidate the matched recipient committee belongs to ('NOT A CC' if it's a non-candidate committee).",
}

# Which script/function produced each *_normalized column, and (briefly) which
# rule categories from that function apply. functions/name_normalization.py is
# frozen (additive changes only) -- this is documentation of it, not a copy of
# its logic, so it can go stale if that file changes; check it directly if
# something here looks off.
NORMALIZATION_INFO = {
    "candidate_name_normalized": (
        "Script: functions/name_normalization.py, normalize_name(name, is_individual=True), "
        "called from processors/ScheduleABCDFI_processor.py and processors/scheduleh_processor.py "
        "at load time. Rules applied: uppercase + collapse whitespace; strip political/personal/"
        "military honorifics (DELEGATE, SENATOR, HON., DR., etc.); reduce to first + last name "
        "only (drops middle names/initials, keeps suffixes like JR/III); hard-coded fix "
        "'CAT PORTERFIELD' -> 'CATHY PORTERFIELD'."
    ),
    "committee_name_normalized": (
        "Script: functions/name_normalization.py, normalize_name(name, is_individual=False), "
        "called from processors/ScheduleABCDFI_processor.py and processors/scheduleh_processor.py "
        "at load time. Rules applied: uppercase + collapse whitespace; ASSOCIATION/ASSN -> ASSOC, "
        "VIRGINIA -> VA, HIGHWAY -> HWY, street-type abbreviations (STREET -> ST, etc.); strip "
        "punctuation and a trailing PAC/INC/CO/CORP/LLC; hard-coded entity fixes -- about 30 known "
        "Dominion Energy name variants collapse to 'DOMINION ENERGY', about 6 Clean Virginia "
        "variants collapse to 'CLEAN VA FUND'."
    ),
    "entity_name_normalized": (
        "Script: functions/name_normalization.py, normalize_name(name, is_individual=<row's "
        "IsIndividual flag>), called from processors/ScheduleABCDFI_processor.py at load time -- "
        "same rule set as committee_name_normalized/candidate_name_normalized above, chosen "
        "per-row depending on whether the source data flagged this entity as a person or an "
        "organization."
    ),
}


def extract_header_comment(sql_text: str) -> str:
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            lines.append(stripped[2:].strip())
        else:
            break
    return " ".join(l for l in lines if l)


def extract_dependencies(sql_text: str, self_name: str) -> list[str]:
    deps = []
    seen = set()
    for placeholder, name in re.findall(r"\{\{(silver_dataset|gold_dataset)\}\}\.(\w+)", sql_text):
        if placeholder == "gold_dataset" and name == self_name:
            continue
        layer = "silver" if placeholder == "silver_dataset" else "gold"
        label = f"{name} ({layer})"
        if label not in seen:
            seen.add(label)
            deps.append(label)
    return deps


@st.cache_data(ttl=3600)
def load_table_docs() -> dict[str, dict]:
    """One entry per gold table, sourced from sql/01_dims .. sql/04_data_quality."""
    docs = {}
    for sql_file in sorted(SQL_ROOT.glob("0[1-4]_*/*.sql")):
        name = sql_file.stem
        text = sql_file.read_text()
        docs[name] = {
            "description": extract_header_comment(text),
            "depends_on": extract_dependencies(text, self_name=name),
            "sql_path": str(sql_file.relative_to(SQL_ROOT.parent)),
        }
    return docs


@st.cache_data(ttl=3600)
def load_helper_docs() -> dict[str, dict]:
    docs = {}
    for sql_file in sorted(SQL_ROOT.glob("00_setup/*.sql")):
        text = sql_file.read_text()
        docs[sql_file.stem] = {
            "description": extract_header_comment(text),
            "sql_path": str(sql_file.relative_to(SQL_ROOT.parent)),
        }
    return docs


@st.cache_data(ttl=300)
def load_live_columns(_client) -> dict[str, list[tuple[str, str]]]:
    """table_name -> [(column_name, data_type), ...], straight from BigQuery."""
    query = f"""
        SELECT table_name, column_name, data_type
        FROM `{PROJECT_ID}.{GOLD_DATASET}.INFORMATION_SCHEMA.COLUMNS`
        ORDER BY table_name, ordinal_position
    """
    rows = _client.query(query).result()
    columns: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        columns.setdefault(row.table_name, []).append((row.column_name, row.data_type))
    return columns


def build_copyable_reference(table_docs: dict, helper_docs: dict, live_columns: dict) -> str:
    lines = [
        "# Virginia Campaign Finance -- Gold Dataset Reference",
        "",
        "Generated live from the actual BigQuery schema each time this page loads --",
        "column names/types are always current. Table descriptions come from the ",
        "SQL that builds each table.",
        "",
        "## Pipeline",
        "1. Bronze -- ingest/download_to_gcs.py scrapes Virginia SBE campaign finance",
        "   CSVs into a GCS bucket (raw_data/), unmodified.",
        "2. Silver -- processors/*.py parse and clean those CSVs into BigQuery dataset",
        "   `virginia_elections` (tables: campaign_finance, schedule_h, cf_clean,",
        "   committee_mappings, name_variations), applying name normalization from",
        "   functions/name_normalization.py.",
        f"3. Gold -- sql/*.sql (run via sql/run_gold.py) build `{GOLD_DATASET}`, the",
        "   dimensional/fact/mart/data-quality tables below. This is the only layer",
        "   queryable through this app.",
        "",
        "## Shared helper functions",
    ]
    for helper_name, info in helper_docs.items():
        lines.append(f"### {helper_name}  ({info['sql_path']})")
        lines.append(info["description"])
        lines.append("")

    lines.append("## Tables")
    for table_name, info in table_docs.items():
        lines.append(f"### {table_name}  ({info['sql_path']})")
        lines.append(info["description"])
        if info["depends_on"]:
            lines.append(f"Depends on: {', '.join(info['depends_on'])}")
        lines.append("Columns:")
        for col_name, col_type in live_columns.get(table_name, []):
            definition = COLUMN_DEFINITIONS.get(col_name, "(no definition yet)")
            lines.append(f"- {col_name} ({col_type}): {definition}")
            if col_name in NORMALIZATION_INFO:
                lines.append(f"    Normalization: {NORMALIZATION_INFO[col_name]}")
        if table_name not in live_columns:
            lines.append("- (table not found in BigQuery yet -- run sql/run_gold.py)")
        lines.append("")

    
    return "\n".join(lines)


def main():
    st.title("Documentation")
    require_project_id()

    try:
        client = get_bigquery_client()
        live_columns = load_live_columns(client)
    except DefaultCredentialsError:
        st.error(AUTH_HELP)
        return
    except Exception as e:
        st.error(f"Couldn't reach BigQuery to load live column info: {e}")
        return

    table_docs = load_table_docs()
    helper_docs = load_helper_docs()

    st.caption(
        "Column names and types below come from BigQuery's live schema. Table "
        "descriptions come from the SQL files that build each table (sql/), so both "
        "stay accurate as the pipeline changes. Column definitions and normalization "
        "notes are hand-written and may lag a brand-new column."
    )

    st.header("Pipeline")
    st.markdown(
        expand_acronyms(
            "1. **Bronze** -- `ingest/download_to_gcs.py` scrapes VA SBE campaign finance "
            "CSVs into GCS, unmodified.\n"
        )
        + expand_acronyms(
            "2. **Silver** -- `processors/*.py` parse/clean those CSVs into BigQuery "
            "dataset `virginia_elections`, applying name normalization.\n"
        )
        + expand_acronyms(
            f"3. **Gold** -- `sql/*.sql` (via `sql/run_gold.py`) build `{GOLD_DATASET}`, "
            "shown below. This is the only layer this app queries."
        )
    )

    if helper_docs:
        st.header("Shared helper functions")
        for helper_name, info in helper_docs.items():
            with st.expander(f"`{helper_name}`  —  {info['sql_path']}"):
                st.write(expand_acronyms(info["description"]))

    st.header("Tables")
    for table_name, info in table_docs.items():
        cols = live_columns.get(table_name)
        label = f"`{table_name}`" + ("" if cols else "  ⚠️ not found in BigQuery yet")
        with st.expander(label):
            st.write(expand_acronyms(info["description"]))
            if info["depends_on"]:
                st.caption(f"Depends on: {', '.join(info['depends_on'])}")
            st.caption(f"Defined in `{info['sql_path']}`")
            if cols:
                st.dataframe(
                    {
                        "column": [c for c, _ in cols],
                        "type": [t for _, t in cols],
                        "definition": [
                            expand_acronyms(COLUMN_DEFINITIONS.get(c, "(no definition yet)"))
                            for c, _ in cols
                        ],
                        "normalization": [
                            expand_acronyms(NORMALIZATION_INFO.get(c, "")) for c, _ in cols
                        ],
                    },
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("Run `sql/run_gold.py` to create this table.")

    st.header("Copy for an LLM")
    st.caption(
        "Don't know SQL?"
        "---"
        "No problem! Select all, copy, and paste it into ChatGPT, Claude, "
        "or any other chat AI, along with a question like"
        '"write a BigQuery SQL query that finds the top 10 donors to <candidate name>".'
        "Then paste the SQL it gives you into the search box on the main page of this"
        "app -- anything starting with SELECT runs directly against this dataset."
    )
    full_reference = build_copyable_reference(table_docs, helper_docs, live_columns)
    st.code(full_reference, language="markdown")


if __name__ == "__main__":
    main()
