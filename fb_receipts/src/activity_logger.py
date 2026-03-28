"""
Activity logger: records a structured history of every run, per ad account.

Each run appends one entry to activity_log.json:
  {
    "run_id":       "2026-03-28T10:00:00.123456",
    "started_at":   "2026-03-28T10:00:00.123456",
    "completed_at": "2026-03-28T10:01:30.456789",
    "period_start": "2026-03-21",
    "period_end":   "2026-03-28",
    "dry_run":      false,
    "summary":      {"sent": 3, "failed": 0, "skipped": 1, "no_receipts": 0},
    "clients": [
      {
        "client_name":       "Acme Corp",
        "ad_account_id":     "123456789",
        "status":            "sent",        # sent | failed | skipped | no_receipts
        "skip_reason":       null,          # wrong_day | already_sent | dry_run
        "recipients":        ["bob@acme.com"],
        "failed_recipients": [],
        "pdf_count":         2,
        "receipt_count":     1,
        "sent_at":           "2026-03-28T10:01:00.000000",
        "error":             null
      }
    ]
  }
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

ACTIVITY_LOG_FILE = Path("activity_log.json")


class ActivityRun:
    """Tracks all events for a single orchestrator run."""

    def __init__(self, period_start: datetime, period_end: datetime, dry_run: bool = False):
        self.run_id = datetime.now().isoformat()
        self.started_at = self.run_id
        self.completed_at: str | None = None
        self.period_start = period_start.strftime("%Y-%m-%d")
        self.period_end = period_end.strftime("%Y-%m-%d")
        self.dry_run = dry_run
        self.summary: dict = {}
        self._clients: list[dict] = []

    # ── Per-client recording ───────────────────────────────────────────────────

    def record_skipped(
        self,
        client_name: str,
        ad_account_id: str,
        reason: str,
        recipients: list[str],
    ) -> None:
        self._clients.append({
            "client_name": client_name,
            "ad_account_id": ad_account_id,
            "status": "skipped",
            "skip_reason": reason,
            "recipients": recipients,
            "failed_recipients": [],
            "pdf_count": 0,
            "receipt_count": 0,
            "sent_at": None,
            "error": None,
        })

    def record_no_receipts(
        self,
        client_name: str,
        ad_account_id: str,
        recipients: list[str],
    ) -> None:
        self._clients.append({
            "client_name": client_name,
            "ad_account_id": ad_account_id,
            "status": "no_receipts",
            "skip_reason": None,
            "recipients": recipients,
            "failed_recipients": [],
            "pdf_count": 0,
            "receipt_count": 0,
            "sent_at": None,
            "error": None,
        })

    def record_failed(
        self,
        client_name: str,
        ad_account_id: str,
        recipients: list[str],
        error: str,
    ) -> None:
        self._clients.append({
            "client_name": client_name,
            "ad_account_id": ad_account_id,
            "status": "failed",
            "skip_reason": None,
            "recipients": recipients,
            "failed_recipients": recipients,
            "pdf_count": 0,
            "receipt_count": 0,
            "sent_at": None,
            "error": error,
        })

    def record_sent(
        self,
        client_name: str,
        ad_account_id: str,
        recipients: list[str],
        failed_recipients: list[str],
        pdf_count: int,
        receipt_count: int,
    ) -> None:
        self._clients.append({
            "client_name": client_name,
            "ad_account_id": ad_account_id,
            "status": "sent",
            "skip_reason": None,
            "recipients": recipients,
            "failed_recipients": failed_recipients,
            "pdf_count": pdf_count,
            "receipt_count": receipt_count,
            "sent_at": datetime.now().isoformat(),
            "error": None,
        })

    # ── Finalise and persist ───────────────────────────────────────────────────

    def finish(self, summary: dict) -> None:
        self.completed_at = datetime.now().isoformat()
        self.summary = summary

    def save(self) -> None:
        """Append this run to activity_log.json."""
        entry = {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "dry_run": self.dry_run,
            "summary": self.summary,
            "clients": self._clients,
        }
        history = _load_history()
        history.append(entry)
        try:
            with open(ACTIVITY_LOG_FILE, "w") as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            logger.warning("Could not save activity log: %s", e)


# ── Reading history ────────────────────────────────────────────────────────────

def _load_history() -> list:
    if ACTIVITY_LOG_FILE.exists():
        try:
            with open(ACTIVITY_LOG_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not read activity log: %s", e)
    return []


def show_history(n: int = 10, account_id: str | None = None) -> None:
    """Print the last *n* runs, optionally filtered to one ad account."""
    history = _load_history()
    if not history:
        print("No activity history found (activity_log.json does not exist yet).")
        return

    runs = history[-n:][::-1]  # most recent first

    for run in runs:
        dry_tag = "  [DRY RUN]" if run.get("dry_run") else ""
        print(
            f"\n{'─' * 60}\n"
            f"Run:     {run['started_at']}{dry_tag}\n"
            f"Period:  {run['period_start']}  →  {run['period_end']}\n"
            f"Summary: {run.get('summary', {})}"
        )

        clients = run.get("clients", [])
        if account_id:
            clients = [c for c in clients if c["ad_account_id"] == account_id]

        if not clients:
            if account_id:
                print(f"  (no activity for account {account_id} this run)")
            continue

        # Column widths
        name_w = max(len(c["client_name"]) for c in clients)
        name_w = max(name_w, 11)

        print(f"\n  {'Client':<{name_w}}  {'Account ID':<16}  {'Status':<11}  {'PDFs':>4}  Details")
        print(f"  {'─' * name_w}  {'─' * 16}  {'─' * 11}  {'─' * 4}  {'─' * 30}")

        for c in clients:
            status = c["status"].upper()
            detail = ""
            if c["status"] == "sent":
                detail = ", ".join(c["recipients"])
                if c["failed_recipients"]:
                    detail += f"  [FAILED: {', '.join(c['failed_recipients'])}]"
            elif c["status"] == "skipped":
                detail = c.get("skip_reason") or ""
            elif c["status"] == "failed":
                detail = c.get("error") or ""

            print(
                f"  {c['client_name']:<{name_w}}  "
                f"{c['ad_account_id']:<16}  "
                f"{status:<11}  "
                f"{c['pdf_count']:>4}  "
                f"{detail}"
            )

    print(f"\n{'─' * 60}")
    print(f"Showing {len(runs)} of {len(history)} total run(s).  Use --history N to see more.")
