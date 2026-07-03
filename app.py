#!/usr/bin/env python3
"""
Virginia Campaign Finance — search bar that doubles as a SQL editor over the
`virginia_elections_gold` BigQuery dataset (see sql/ for how gold is built).
"""

import concurrent.futures

import pandas as pd
import streamlit as st
from google.cloud import bigquery

from bq_client import (
    AUTH_HELP,
    GOLD_DATASET,
    LAST_UPDATED,
    DefaultCredentialsError,
    get_bigquery_client,
    require_project_id,
)

st.set_page_config(page_title="VA Campaign Finance", layout="wide")

MAX_BYTES_SCANNED = 500_000_000  # reject queries that would scan more than this
MAX_BYTES_BILLED = 1_000_000_000  # hard billing cap per query
ROW_LIMIT = 1000
QUERY_TIMEOUT_SECONDS = 30
FORBIDDEN_KEYWORDS = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "MERGE", "TRUNCATE", "GRANT"]

SAMPLE_QUERIES = {
    "Top Dominion Energy recipients": f"""SELECT candidate_name_normalized, committee_name_normalized, SUM(total_amount) AS total
FROM `{GOLD_DATASET}.top_donors`
WHERE entity_name_normalized LIKE '%DOMINION%'
GROUP BY 1, 2
ORDER BY total DESC
LIMIT 25""",
    "Unmatched contributions": f"""SELECT *
FROM `{GOLD_DATASET}.unmatched_contributions`
ORDER BY amount DESC
LIMIT 100""",
    "Local race cost comparison": f"""SELECT *
FROM `{GOLD_DATASET}.local_election_costs`
ORDER BY total_disbursements DESC
LIMIT 50""",
    "Balance continuity failures": f"""SELECT *
FROM `{GOLD_DATASET}.balance_continuity_failures`
ORDER BY ABS(balance_discrepancy) DESC
LIMIT 50""",
}


def is_safe_select(sql: str) -> tuple[bool, str]:
    statements = [s for s in sql.split(";") if s.strip()]
    if len(statements) != 1:
        return False, "Only a single statement is allowed."
    stripped = statements[0].strip().upper()
    if not stripped.startswith(("SELECT", "WITH")):
        return False, "Only SELECT queries are allowed."
    padded = f" {stripped} "
    if any(f" {kw} " in padded for kw in FORBIDDEN_KEYWORDS):
        return False, "Only read-only SELECT queries are allowed."
    return True, ""


def run_query(client: bigquery.Client, sql: str) -> pd.DataFrame:
    dry_run_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(sql, job_config=dry_run_config)
    if dry_job.total_bytes_processed and dry_job.total_bytes_processed > MAX_BYTES_SCANNED:
        raise ValueError(
            f"Query would scan {dry_job.total_bytes_processed / 1e9:.2f} GB, over the "
            f"{MAX_BYTES_SCANNED / 1e9:.2f} GB cap. Add filters and try again."
        )

    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)
    query_job = client.query(sql, job_config=job_config)
    try:
        rows = query_job.result(max_results=ROW_LIMIT, timeout=QUERY_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        query_job.cancel()
        raise TimeoutError(f"Query timed out after {QUERY_TIMEOUT_SECONDS}s.")
    return rows.to_dataframe()


def search_names(client: bigquery.Client, term: str) -> dict[str, pd.DataFrame]:
    like_term = f"%{term.upper()}%"
    searches = {
        "Candidates": f"""SELECT candidate_name_normalized, office_sought_normal, district_normal,
       level, party, most_recent_election_cycle
FROM `{GOLD_DATASET}.dim_candidate`
WHERE UPPER(candidate_name_normalized) LIKE @like_term
LIMIT 50""",
        "Committees": f"""SELECT committee_code, committee_name_normalized, candidate_name_normalized,
       committee_type, party
FROM `{GOLD_DATASET}.dim_committee`
WHERE UPPER(committee_name_normalized) LIKE @like_term
LIMIT 50""",
        "Donors/Vendors": f"""SELECT entity_name_normalized, is_individual, entity_employer, entity_occupation
FROM `{GOLD_DATASET}.dim_entity`
WHERE UPPER(entity_name_normalized) LIKE @like_term
LIMIT 50""",
    }
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("like_term", "STRING", like_term)],
        maximum_bytes_billed=MAX_BYTES_BILLED,
    )
    results = {}
    for label, sql in searches.items():
        job = client.query(sql, job_config=job_config)
        results[label] = job.result(timeout=QUERY_TIMEOUT_SECONDS).to_dataframe()
    return results


def show_table(df: pd.DataFrame, key: str):
    if df.empty:
        st.info("No results.")
        return
    st.dataframe(df, use_container_width=True)
    st.download_button(
        "Download CSV", df.to_csv(index=False), file_name=f"{key}.csv",
        mime="text/csv", key=f"dl_{key}",
    )


def main():
    st.title("Virginia Campaign Finance")
    st.caption(f"Data last updated: {LAST_UPDATED}")
    require_project_id()

    with st.sidebar:
        st.header("Sample queries")
        for label, sql in SAMPLE_QUERIES.items():
            if st.button(label, use_container_width=True):
                st.session_state["query_input"] = sql

    query_input = st.text_area(
        "Search a name, or type a SELECT query against the gold dataset",
        key="query_input",
        height=100,
        placeholder="youngkin  --or--  SELECT * FROM top_donors LIMIT 10",
    )

    if not query_input.strip():
        return

    try:
        client = get_bigquery_client()
    except DefaultCredentialsError:
        st.error(AUTH_HELP)
        return

    if query_input.strip().upper().startswith(("SELECT", "WITH")):
        safe, reason = is_safe_select(query_input)
        if not safe:
            st.error(reason)
            return
        try:
            with st.spinner("Running query..."):
                df = run_query(client, query_input)
        except (ValueError, TimeoutError) as e:
            st.error(str(e))
            return
        except Exception as e:
            st.error(f"Query failed: {e}")
            return
        st.caption(f"Showing up to {ROW_LIMIT} rows")
        show_table(df, "query_result")
    else:
        with st.spinner("Searching..."):
            try:
                results = search_names(client, query_input.strip())
            except Exception as e:
                st.error(f"Search failed: {e}")
                return
        for label, df in results.items():
            st.subheader(label)
            show_table(df, label.lower().replace("/", "_"))


if __name__ == "__main__":
    main()
