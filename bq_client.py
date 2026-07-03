#!/usr/bin/env python3
"""
Shared BigQuery client/config for app.py and pages/*.py (Streamlit's multipage
convention runs every file under pages/ as its own script, so this module is
what keeps auth/config in one place instead of duplicated per page).
"""

import os

import streamlit as st
from google.auth.exceptions import DefaultCredentialsError  # noqa: F401 (re-exported)
from google.cloud import bigquery
from google.oauth2 import service_account


def get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


# project_id/gold_dataset aren't sensitive (project IDs are public/visible to
# anyone who can see the deployed app's queries) -- hardcoded defaults here,
# still overridable by env var or secrets.toml if you ever point this at a
# different project (e.g. a staging one).
PROJECT_ID = os.environ.get("VA_CF_PROJECT_ID") or get_secret("project_id", "va-campaign-finance")
GOLD_DATASET = os.environ.get("VA_CF_GOLD_DATASET") or get_secret("gold_dataset", "virginia_elections_gold")
LAST_UPDATED = os.environ.get("VA_CF_LAST_UPDATED") or get_secret("last_updated", "unknown")

AUTH_HELP = (
    "BigQuery auth failed. Locally: run `gcloud auth application-default login` "
    "once. Deployed: add a `[gcp_service_account]` block in this app's Secrets "
    "settings on share.streamlit.io (never in a file in this repo)."
)


@st.cache_resource
def get_bigquery_client() -> bigquery.Client:
    # Only present when Streamlit Community Cloud injects it from that app's own
    # Secrets dashboard (share.streamlit.io) -- never a file in this repo. Locally
    # this secret won't exist, so we fall back to Application Default Credentials:
    # run `gcloud auth application-default login` yourself once, and the client
    # picks it up with no key material touching this project at all.
    creds_info = get_secret("gcp_service_account")
    if creds_info:
        credentials = service_account.Credentials.from_service_account_info(creds_info)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)


def require_project_id():
    if not PROJECT_ID:
        st.warning(
            "Set the `VA_CF_PROJECT_ID` environment variable (or `project_id` in "
            "`.streamlit/secrets.toml`) to your GCP project id."
        )
        st.stop()
