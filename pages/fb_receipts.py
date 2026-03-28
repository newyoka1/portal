"""Facebook Ads Receipt Automation page — adapted from facebook-receipt-automation/app.py."""
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

PROJECT_DIR = Path(r"D:\git\facebook-receipt-automation")
sys.path.insert(0, str(PROJECT_DIR))
load_dotenv(PROJECT_DIR / ".env")

PYTHON = sys.executable

SCHEDULE_OPTIONS = [
    "weekly_monday", "weekly_tuesday", "weekly_wednesday",
    "weekly_thursday", "weekly_friday", "weekly_saturday", "weekly_sunday",
    "monthly_1", "monthly_7", "monthly_14", "monthly_15", "monthly_28",
]
TIME_OPTIONS    = [f"{h:02d}:00" for h in range(6, 21)]
ACTIVE_OPTIONS  = ["yes", "no"]


def get_sheets_client():
    from src.sheets_client import SheetsClient
    return SheetsClient()


def run_command(args: list[str]) -> int:
    log_area = st.empty()
    lines = []
    with subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, cwd=str(PROJECT_DIR),
    ) as proc:
        for line in proc.stdout:
            lines.append(line.rstrip())
            log_area.code("\n".join(lines[-60:]), language="log")
    return proc.returncode


st.title("🧾 Facebook Ads Receipt Automation")

tab_run, tab_custom, tab_clients, tab_settings, tab_scheduler, tab_log = st.tabs([
    "▶  Run Receipts", "🎯  Custom Run", "👥  Clients", "⚙️  Settings", "🗓  Scheduler", "📋  Sent Log",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RUN RECEIPTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_run:
    st.subheader("Send Receipts")

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start date", datetime.now() - timedelta(days=7))
    end_date   = col2.date_input("End date",   datetime.now())

    col3, col4, col5 = st.columns(3)
    dry_run    = col3.checkbox("Dry run (no emails sent)")
    resend     = col4.checkbox("Resend (ignore sent log)")
    no_fb_pdfs = col5.checkbox("Skip Facebook PDF download")

    st.divider()

    col_run, col_last = st.columns([3, 1])
    run_clicked  = col_run.button("🚀 Send Receipts" + (" — DRY RUN" if dry_run else ""),
                                   type="primary", use_container_width=True)
    last_clicked = col_last.button("Last run info", use_container_width=True)

    if last_clicked:
        r = subprocess.run([PYTHON, str(PROJECT_DIR / "main.py"), "--last-run"],
                           capture_output=True, text=True, cwd=str(PROJECT_DIR))
        st.info(r.stdout or r.stderr)

    if run_clicked:
        if start_date > end_date:
            st.error("Start date must be before end date.")
        else:
            args = [PYTHON, str(PROJECT_DIR / "main.py"),
                    "--start-date", start_date.strftime("%Y-%m-%d"),
                    "--end-date",   end_date.strftime("%Y-%m-%d")]
            if dry_run:    args.append("--dry-run")
            if resend:     args.append("--resend")
            if no_fb_pdfs: args.append("--no-fb-pdfs")

            st.subheader("Output")
            code = run_command(args)
            if code == 0:
                st.success("Run complete.")
            else:
                st.error("Run finished with errors — check output above.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CUSTOM RUN
# ══════════════════════════════════════════════════════════════════════════════
with tab_custom:
    st.subheader("Run for a Single Client")

    @st.cache_data(ttl=30, show_spinner="Loading clients...")
    def load_clients_for_custom():
        try:
            return get_sheets_client().get_all_clients_raw()
        except Exception:
            return []

    custom_clients = load_clients_for_custom()
    all_client_names = [
        f"{c.get('client_name', '?')}  ({c.get('ad_account_id', '')})"
        for c in custom_clients
    ]

    if not all_client_names:
        st.info("No clients found. Check the Clients tab.")
    else:
        selected        = st.selectbox("Select client", all_client_names)
        selected_client = custom_clients[all_client_names.index(selected)]

        col1, col2 = st.columns(2)
        cr_start = col1.date_input("Start date", datetime.now() - timedelta(days=7), key="cr_start")
        cr_end   = col2.date_input("End date",   datetime.now(),                     key="cr_end")

        col3, col4 = st.columns(2)
        cr_dry    = col3.checkbox("Dry run", key="cr_dry")
        cr_resend = col4.checkbox("Resend",  key="cr_resend")

        if st.button(f"🚀 Run — {selected_client.get('client_name', '')}", type="primary"):
            if cr_start > cr_end:
                st.error("Start date must be before end date.")
            else:
                args = [
                    PYTHON, str(PROJECT_DIR / "main.py"),
                    "--start-date", cr_start.strftime("%Y-%m-%d"),
                    "--end-date",   cr_end.strftime("%Y-%m-%d"),
                    "--account-id", selected_client.get("ad_account_id", ""),
                ]
                if cr_dry:    args.append("--dry-run")
                if cr_resend: args.append("--resend")

                st.subheader("Output")
                code = run_command(args)
                st.success("Done.") if code == 0 else st.error("Failed — check output above.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CLIENTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_clients:
    st.subheader("Client List")

    col_reload, col_import, _ = st.columns([1, 1, 3])

    if col_reload.button("↻ Reload from Sheet"):
        st.cache_data.clear()
        st.rerun()

    import_clicked = col_import.button("⬇ Import from Meta API")

    @st.cache_data(ttl=30, show_spinner="Loading clients...")
    def load_all_clients():
        try:
            sc = get_sheets_client()
            return sc.get_all_clients_raw(), sc.get_settings()
        except Exception as e:
            return [], {}, str(e)

    result = load_all_clients()
    if len(result) == 3:
        clients_raw, settings, load_error = result
        st.error(f"Could not load sheet: {load_error}")
    else:
        clients_raw, settings = result
        load_error = None

    if import_clicked:
        with st.spinner("Fetching all ad accounts from Meta API..."):
            r = subprocess.run(
                [PYTHON, str(PROJECT_DIR / "populate_sheet.py")],
                capture_output=True, text=True, cwd=str(PROJECT_DIR),
            )
        if r.returncode == 0:
            st.success("Imported from Meta. Reloading...")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error(r.stderr or r.stdout)

    default_row = {"client_name": "", "ad_account_id": "", "email": "",
                   "active": "no", "schedule": "weekly_friday"}
    if clients_raw:
        df = pd.DataFrame([
            {k: c.get(k, default_row[k]) for k in default_row}
            for c in clients_raw
        ])
    else:
        df = pd.DataFrame(columns=list(default_row.keys()))

    df["active"]   = df["active"].replace("", "no").fillna("no")
    df["schedule"] = df["schedule"].replace("", "weekly_friday").fillna("weekly_friday")

    edited_df = st.data_editor(
        df,
        column_config={
            "client_name":   st.column_config.TextColumn("Client Name",  width="medium"),
            "ad_account_id": st.column_config.TextColumn("Ad Account ID", width="medium"),
            "email":         st.column_config.TextColumn("Email(s)", width="large",
                                 help="Single address or comma-separated"),
            "active":        st.column_config.SelectboxColumn("Active",   options=ACTIVE_OPTIONS, width="small"),
            "schedule":      st.column_config.SelectboxColumn("Schedule", options=SCHEDULE_OPTIONS, width="medium"),
        },
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        key="client_editor",
    )

    st.caption(f"{len(edited_df)} rows — {len(edited_df[edited_df['active']=='yes'])} active")

    if st.button("💾 Save to Google Sheet", type="primary"):
        with st.spinner("Saving..."):
            try:
                sc = get_sheets_client()
                sc.save_sheet_data(settings, edited_df.fillna("").to_dict("records"))
                st.cache_data.clear()
                st.success(f"Saved {len(edited_df)} clients to Google Sheet.")
            except Exception as e:
                st.error(f"Save failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SETTINGS
# ══════════════════════════════════════════════════════════════════════════════
with tab_settings:
    st.subheader("Global Settings")

    @st.cache_data(ttl=60, show_spinner=False)
    def load_settings():
        try:
            return get_sheets_client().get_settings()
        except Exception:
            return {}

    s = load_settings()

    with st.form("settings_form"):
        admin_email      = st.text_input("Admin email (gets a copy of every send)",
                                          value=s.get("admin_email", ""))
        notify_email     = st.text_input("Notify email (failure alerts)",
                                          value=s.get("notify_email", ""))
        schedule_time    = st.selectbox("Default run time", TIME_OPTIONS,
                                         index=TIME_OPTIONS.index(s.get("schedule_time", "09:00"))
                                         if s.get("schedule_time", "09:00") in TIME_OPTIONS else 3)
        default_schedule = st.selectbox("Default client schedule", SCHEDULE_OPTIONS,
                                         index=SCHEDULE_OPTIONS.index(s.get("default_schedule", "weekly_friday"))
                                         if s.get("default_schedule", "weekly_friday") in SCHEDULE_OPTIONS else 4)

        if st.form_submit_button("💾 Save Settings", type="primary"):
            try:
                sc = get_sheets_client()
                sc.save_sheet_data(
                    {"admin_email": admin_email, "notify_email": notify_email,
                     "schedule_time": schedule_time, "default_schedule": default_schedule},
                    sc.get_all_clients_raw(),
                )
                st.cache_data.clear()
                st.success("Settings saved.")
            except Exception as e:
                st.error(f"Save failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════
with tab_scheduler:
    st.subheader("Windows Task Scheduler")
    st.caption("Creates one scheduled task per active client based on their schedule column.")

    if st.button("↻ Sync Scheduler from Sheet", type="primary"):
        st.subheader("Output")
        code = run_command([PYTHON, str(PROJECT_DIR / "main.py"), "--sync-scheduler"])
        if code == 0:
            st.success("Scheduler synced.")
        else:
            st.error("Sync failed — check output above.")

    st.divider()
    st.subheader("Current Tasks")

    if st.button("↻ Refresh task list"):
        pass

    r = subprocess.run(["schtasks", "/query", "/fo", "csv", "/nh"],
                       capture_output=True, text=True)
    fb_tasks = []
    for line in r.stdout.splitlines():
        parts = line.strip('"').split('","')
        if parts and "FacebookReceipt" in parts[0]:
            fb_tasks.append({
                "Task":     parts[0] if len(parts) > 0 else "",
                "Next Run": parts[1] if len(parts) > 1 else "",
                "Status":   parts[2] if len(parts) > 2 else "",
            })

    if fb_tasks:
        st.dataframe(fb_tasks, use_container_width=True, hide_index=True)
    else:
        st.info("No FacebookReceipt tasks found. Click 'Sync Scheduler from Sheet' to create them.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — SENT LOG
# ══════════════════════════════════════════════════════════════════════════════
with tab_log:
    st.subheader("Sent Log")

    log_path = PROJECT_DIR / "sent_log.json"

    col_refresh, col_clear, _ = st.columns([1, 1, 4])
    if col_refresh.button("↻ Refresh"):
        pass

    if col_clear.button("🗑 Clear log", type="secondary"):
        if log_path.exists():
            log_path.write_text("{}")
            st.success("Sent log cleared.")
            st.rerun()

    if not log_path.exists():
        st.info("No sent_log.json found — nothing has been sent yet.")
    else:
        try:
            log_data = json.loads(log_path.read_text())

            if not log_data:
                st.info("Sent log is empty.")
            else:
                try:
                    name_map = {
                        c.get("ad_account_id", ""): c.get("client_name", "")
                        for c in get_sheets_client().get_all_clients_raw()
                    }
                except Exception:
                    name_map = {}

                entries = sorted(log_data.items(),
                                 key=lambda x: x[1].get("sent_at", ""), reverse=True)

                h1, h2, h3, h4, h5, h6 = st.columns([2, 2, 2, 2, 3, 1])
                h1.markdown("**Client**"); h2.markdown("**Account ID**")
                h3.markdown("**Period**"); h4.markdown("**Sent At**")
                h5.markdown("**Recipients**"); h6.markdown("**Action**")
                st.divider()

                for account_id, entry in entries:
                    c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 2, 2, 3, 1])
                    c1.write(name_map.get(account_id, "—"))
                    c2.write(account_id)
                    c3.write(f"{entry.get('period_start','')} → {entry.get('period_end','')}")
                    c4.write(entry.get("sent_at", "")[:19].replace("T", " "))
                    c5.write(", ".join(entry.get("recipients", [])))

                    if c6.button("↺ Resend", key=f"resend_{account_id}"):
                        args = [
                            PYTHON, str(PROJECT_DIR / "main.py"),
                            "--start-date", entry.get("period_start", ""),
                            "--end-date",   entry.get("period_end", ""),
                            "--account-id", account_id, "--resend",
                        ]
                        st.subheader(f"Resending — {name_map.get(account_id, account_id)}")
                        code = run_command(args)
                        st.success("Done.") if code == 0 else st.error("Failed — check output above.")

                st.caption(f"{len(entries)} entries")

        except Exception as e:
            st.error(f"Could not read sent_log.json: {e}")
