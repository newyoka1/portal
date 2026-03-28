"""NYS Voter Pipeline page."""
import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import run_command

VOTER_DIR = Path(r"D:\git\nys-voter-pipeline")
PYTHON    = sys.executable

st.title("🗳️ NYS Voter Pipeline")

tab_status, tab_export, tab_pipeline, tab_donors, tab_crm, tab_sync = st.tabs([
    "📊 Status", "📁 Export", "⚙️ Pipeline", "💰 Donors", "👥 CRM", "☁️ Sync",
])

# ══════════════════════════════════════════════════════════════════════════════
# STATUS
# ══════════════════════════════════════════════════════════════════════════════
with tab_status:
    st.subheader("Data Freshness")
    st.caption("Shows age of source files and last pipeline run times.")
    if st.button("↻ Check Status", type="primary"):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "status"], cwd=str(VOTER_DIR))

# ══════════════════════════════════════════════════════════════════════════════
# EXPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_export:
    st.subheader("Export District to Excel")

    col1, col2 = st.columns(2)
    district_type = col1.selectbox("District type", ["ld", "sd", "cd", "county"])

    if district_type == "county":
        district_value = col2.text_input("County name", "Nassau")
        flag_value     = district_value
    else:
        district_value = col2.number_input("District number", min_value=1, max_value=200, value=63, step=1)
        flag_value     = str(int(district_value))

    voter_contact = st.checkbox("Voter-contact format (adds contact methods + party tabs)")

    if st.button("▶ Run Export", type="primary"):
        cmd = "voter-contact" if voter_contact else "export"
        flag = f"--{district_type}"
        args = [PYTHON, str(VOTER_DIR / "main.py"), cmd, flag, str(flag_value)]

        code = run_command(args, cwd=str(VOTER_DIR))

        if code == 0:
            # Find the most recently modified xlsx anywhere under output/
            output_root = VOTER_DIR / "output"
            all_xlsx = sorted(output_root.rglob("*.xlsx"),
                              key=lambda f: f.stat().st_mtime, reverse=True)
            if all_xlsx:
                newest = all_xlsx[0]
                with open(newest, "rb") as f:
                    st.download_button(
                        f"Download {newest.name}", f.read(), newest.name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                    )
            st.success("Export complete.")
        else:
            st.error("Export failed — check output above.")

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    st.subheader("Voter File Pipeline")
    st.warning("These operations are slow (minutes to hours). Do not close the browser tab.", icon="⚠️")

    col_pipe, col_eth, col_derived, col_party = st.columns(4)

    if col_pipe.button("▶ Load All Voters", use_container_width=True, type="primary"):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "pipeline"], cwd=str(VOTER_DIR))

    if col_eth.button("▶ Build Ethnicity Model", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "ethnicity"], cwd=str(VOTER_DIR))

    if col_derived.button("▶ Enrich Derived", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "enrich-derived"], cwd=str(VOTER_DIR))

    if col_party.button("▶ Party Snapshot", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "party-snapshot"], cwd=str(VOTER_DIR))

# ══════════════════════════════════════════════════════════════════════════════
# DONORS
# ══════════════════════════════════════════════════════════════════════════════
with tab_donors:
    st.subheader("Donor Pipelines")

    col1, col2 = st.columns(2)
    refresh = col1.radio("Download fresh data?", ["No (use existing)", "Yes (re-download)"],
                         horizontal=True)

    st.divider()

    col_all, col_boe, col_nat, col_cfb = st.columns(4)

    refresh_flag = [] if "No" in refresh else ["--refresh"]
    no_refresh_flag = ["--no-refresh"] if "No" in refresh else []

    if col_all.button("▶ All Donors", use_container_width=True, type="primary"):
        args = [PYTHON, str(VOTER_DIR / "main.py"), "donors"] + no_refresh_flag + refresh_flag
        run_command(args, cwd=str(VOTER_DIR))

    if col_boe.button("▶ State (BOE)", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "boe-enrich"], cwd=str(VOTER_DIR))

    if col_nat.button("▶ National", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "national-enrich"], cwd=str(VOTER_DIR))

    if col_cfb.button("▶ NYC (CFB)", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "cfb-enrich"], cwd=str(VOTER_DIR))

# ══════════════════════════════════════════════════════════════════════════════
# CRM
# ══════════════════════════════════════════════════════════════════════════════
with tab_crm:
    st.subheader("CRM Sync")

    full = st.checkbox("Full re-sync (slower, re-processes all records)")
    full_flag = ["--full"] if full else []

    st.divider()

    col1, col2, col3, col4, col5 = st.columns(5)

    if col1.button("▶ All CRM", use_container_width=True, type="primary"):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "crm-sync"] + full_flag, cwd=str(VOTER_DIR))

    if col2.button("▶ HubSpot", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "hubspot-sync"] + full_flag, cwd=str(VOTER_DIR))

    if col3.button("▶ Campaign Monitor", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "cm-sync"] + full_flag, cwd=str(VOTER_DIR))

    if col4.button("▶ Mailchimp", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "mailchimp-sync"] + full_flag, cwd=str(VOTER_DIR))

    if col5.button("▶ Enrich Contacts", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "crm-enrich"] + full_flag, cwd=str(VOTER_DIR))

# ══════════════════════════════════════════════════════════════════════════════
# SYNC
# ══════════════════════════════════════════════════════════════════════════════
with tab_sync:
    st.subheader("Sync to Aiven")
    st.caption("Pushes enriched local MySQL tables to the Aiven cloud replica.")

    col1, col2, col3 = st.columns(3)

    if col1.button("▶ Sync Voter Tables", use_container_width=True, type="primary"):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "sync"], cwd=str(VOTER_DIR))

    if col2.button("▶ Sync All Databases", use_container_width=True):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "sync", "--all-databases"], cwd=str(VOTER_DIR))

    st.divider()
    st.subheader("Sync Specific Databases")
    dbs = st.multiselect("Choose databases", ["boe_donors", "National_Donors", "cfb_donors", "crm_unified"])
    if dbs and st.button("▶ Sync Selected", type="primary"):
        run_command([PYTHON, str(VOTER_DIR / "main.py"), "sync", "--databases"] + dbs, cwd=str(VOTER_DIR))
