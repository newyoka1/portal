"""
MySQL-backed client/settings store — replaces SheetsClient.

Database: fb_receipts  (Aiven MySQL)
Tables:
  clients  — one row per ad account
  settings — key/value global config
"""

import logging
import os
from pathlib import Path

import pymysql
import pymysql.cursors

logger = logging.getLogger(__name__)

# ── Connection config from env ─────────────────────────────────────────────────
_PROJECT_DIR = Path(__file__).resolve().parent.parent   # facebook-receipt-automation/

def _ssl_ca() -> str | None:
    raw = os.environ.get("FB_AIVEN_SSL_CA", "")
    if not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = _PROJECT_DIR / p
    return str(p) if p.exists() else None


def _get_conn() -> pymysql.connections.Connection:
    ssl_ca = _ssl_ca()
    kwargs = dict(
        host=os.environ.get("FB_AIVEN_HOST") or os.environ.get("MYSQL_HOST", ""),
        user=os.environ.get("FB_AIVEN_USER") or os.environ.get("MYSQL_USER", ""),
        password=os.environ.get("FB_AIVEN_PASSWORD") or os.environ.get("MYSQL_PASSWORD", ""),
        port=int(os.environ.get("FB_AIVEN_PORT") or os.environ.get("MYSQL_PORT", 3306)),
        database=os.environ.get("FB_AIVEN_DB", "fb_receipts"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )
    if ssl_ca:
        kwargs["ssl"] = {"ca": ssl_ca}
    return pymysql.connect(**kwargs)


# ── Public client class ────────────────────────────────────────────────────────

class DbClient:
    """Drop-in replacement for SheetsClient backed by Aiven MySQL."""

    # ── Settings ──────────────────────────────────────────────────────────────

    def get_settings(self) -> dict:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT setting_key, setting_value FROM settings")
                rows = cur.fetchall()
        settings = {r["setting_key"]: r["setting_value"] for r in rows}
        logger.info("Loaded %d setting(s) from DB", len(settings))
        return settings

    def save_settings(self, settings: dict) -> None:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                for key, val in settings.items():
                    cur.execute(
                        """
                        INSERT INTO settings (setting_key, setting_value)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)
                        """,
                        (key, val),
                    )
            conn.commit()
        logger.info("Saved %d setting(s) to DB", len(settings))

    # ── Clients ───────────────────────────────────────────────────────────────

    def get_all_clients_raw(self) -> list[dict]:
        """All rows (active + inactive) — for the admin UI."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT client_name, ad_account_id, email, active, schedule "
                    "FROM clients ORDER BY client_name"
                )
                rows = cur.fetchall()
        return [dict(r) for r in rows]

    def get_client_mappings(self) -> list[dict]:
        """Active clients only — used by the orchestrator."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT client_name, ad_account_id, email, schedule "
                    "FROM clients WHERE active = 'yes' ORDER BY client_name"
                )
                rows = cur.fetchall()

        mappings = []
        for r in rows:
            emails = [e.strip() for e in r["email"].split(",") if e.strip()]
            if not r["ad_account_id"] or not emails:
                logger.warning("Skipping client with missing account/email: %s", r)
                continue
            mappings.append({
                "client_name":   r["client_name"],
                "ad_account_id": r["ad_account_id"],
                "emails":        emails,
                "email":         emails[0],
                "schedule":      r["schedule"] or "weekly_friday",
            })

        logger.info("Loaded %d active client mapping(s) from DB", len(mappings))
        return mappings

    def save_clients(self, clients: list[dict]) -> None:
        """Upsert all clients. Rows absent from the list are NOT deleted."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                for c in clients:
                    cur.execute(
                        """
                        INSERT INTO clients
                            (client_name, ad_account_id, email, active, schedule)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            client_name = VALUES(client_name),
                            email       = VALUES(email),
                            active      = VALUES(active),
                            schedule    = VALUES(schedule)
                        """,
                        (
                            c.get("client_name", ""),
                            c.get("ad_account_id", ""),
                            c.get("email", ""),
                            c.get("active", "no"),
                            c.get("schedule", "weekly_friday"),
                        ),
                    )
                # Remove rows whose ad_account_id is no longer in the list
                ids = [c.get("ad_account_id", "") for c in clients if c.get("ad_account_id")]
                if ids:
                    placeholders = ",".join(["%s"] * len(ids))
                    cur.execute(
                        f"DELETE FROM clients WHERE ad_account_id NOT IN ({placeholders})",
                        ids,
                    )
            conn.commit()
        logger.info("Saved %d client(s) to DB", len(clients))

    # ── Combined save (UI calls this) ─────────────────────────────────────────

    def save_all(self, settings: dict, clients: list[dict]) -> None:
        """Combined save — settings + clients in one call."""
        self.save_settings(settings)
        self.save_clients(clients)

    # ── Sent Receipts ───────────────────────────────────────────────────────

    def delete_receipt(self, receipt_id: int) -> None:
        """Delete a receipt from the database."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sent_receipts WHERE id = %s", (receipt_id,))
            conn.commit()

    def delete_all_receipts(self) -> None:
        """Delete all receipts from the database."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sent_receipts")
            conn.commit()

    def is_receipt_sent(self, ad_account_id: str, transaction_id: str) -> bool:
        """Check if a receipt has already been processed."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM sent_receipts "
                    "WHERE ad_account_id = %s AND transaction_id = %s",
                    (ad_account_id, transaction_id),
                )
                return cur.fetchone() is not None

    def save_sent_receipt(self, receipt: dict, pdf_data: bytes, pdf_filename: str,
                          sent_to: str, status: str = "sent", error: str = "",
                          ad_images_json: str = "") -> int:
        """Store a sent receipt with PDF + ad images for future resend."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sent_receipts
                        (ad_account_id, transaction_id, gmail_message_id, receipt_for,
                         amount, currency, invoice_date, date_range_start, date_range_end,
                         payment_method, reference_number, billing_reason, product_type,
                         email_subject, pdf_data, pdf_filename, ad_images_json,
                         sent_to, sent_at, status, error)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
                    ON DUPLICATE KEY UPDATE
                        pdf_data = VALUES(pdf_data),
                        pdf_filename = VALUES(pdf_filename),
                        ad_images_json = VALUES(ad_images_json),
                        sent_to = VALUES(sent_to),
                        sent_at = NOW(),
                        status = VALUES(status),
                        error = VALUES(error)
                    """,
                    (
                        receipt.get("account_id", ""),
                        receipt.get("transaction_id", ""),
                        receipt.get("gmail_message_id", ""),
                        receipt.get("receipt_for", ""),
                        receipt.get("amount", 0),
                        receipt.get("currency", "USD"),
                        receipt.get("email_date") or None,
                        receipt.get("date_range_start", ""),
                        receipt.get("date_range_end", ""),
                        receipt.get("payment_method", ""),
                        receipt.get("reference_number", ""),
                        receipt.get("billing_reason", ""),
                        receipt.get("product_type", "Meta ads"),
                        receipt.get("email_subject", ""),
                        pdf_data,
                        pdf_filename,
                        ad_images_json,
                        sent_to,
                        status,
                        error,
                    ),
                )
            conn.commit()
            return cur.lastrowid

    def get_sent_receipts(self, ad_account_id: str | None = None,
                          limit: int = 50) -> list[dict]:
        """Get sent receipt history (without PDF binary for listing)."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                sql = (
                    "SELECT id, ad_account_id, transaction_id, receipt_for, amount, "
                    "currency, invoice_date, date_range_start, date_range_end, "
                    "payment_method, pdf_filename, sent_to, sent_at, status, error "
                    "FROM sent_receipts "
                )
                if ad_account_id:
                    sql += "WHERE ad_account_id = %s "
                    sql += "ORDER BY sent_at DESC LIMIT %s"
                    cur.execute(sql, (ad_account_id, limit))
                else:
                    sql += "ORDER BY sent_at DESC LIMIT %s"
                    cur.execute(sql, (limit,))
                return [dict(r) for r in cur.fetchall()]

    def get_receipt_pdf(self, receipt_id: int) -> tuple[bytes, str] | None:
        """Get PDF binary + filename for a specific receipt (for resend/download)."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pdf_data, pdf_filename FROM sent_receipts WHERE id = %s",
                    (receipt_id,),
                )
                row = cur.fetchone()
                if row and row.get("pdf_data"):
                    return row["pdf_data"], row["pdf_filename"]
                return None

    def get_receipt_with_images(self, receipt_id: int) -> dict | None:
        """Get PDF + ad images for resend."""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pdf_data, pdf_filename, ad_images_json, receipt_for "
                    "FROM sent_receipts WHERE id = %s",
                    (receipt_id,),
                )
                row = cur.fetchone()
                if row and row.get("pdf_data"):
                    return dict(row)
                return None
