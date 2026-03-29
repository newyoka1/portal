"""
One-time setup: creates the fb_receipts database and tables on Aiven MySQL.
Run once from the facebook-receipt-automation directory:

    python setup_fb_db.py
"""

from pathlib import Path
from dotenv import load_dotenv
import os, pymysql

load_dotenv(Path(__file__).parent / ".env")

host     = os.environ.get("FB_AIVEN_HOST") or os.environ.get("MYSQL_HOST", "")
user     = os.environ.get("FB_AIVEN_USER") or os.environ.get("MYSQL_USER", "")
password = os.environ.get("FB_AIVEN_PASSWORD") or os.environ.get("MYSQL_PASSWORD", "")
port     = int(os.environ.get("FB_AIVEN_PORT") or os.environ.get("MYSQL_PORT", 3306))
ssl_ca   = os.environ.get("FB_AIVEN_SSL_CA", "")
if ssl_ca and not Path(ssl_ca).is_absolute():
    ssl_ca = str(Path(__file__).parent / ssl_ca)

ssl = {"ca": ssl_ca} if ssl_ca and Path(ssl_ca).exists() else {}

print(f"Connecting to {host}:{port} ...")
conn = pymysql.connect(host=host, user=user, password=password, port=port,
                       ssl=ssl or None, connect_timeout=15)

with conn.cursor() as cur:
    cur.execute("CREATE DATABASE IF NOT EXISTS fb_receipts CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    print("Database fb_receipts: ready")

    cur.execute("USE fb_receipts")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            client_name     VARCHAR(255) NOT NULL DEFAULT '',
            ad_account_id   VARCHAR(64)  NOT NULL DEFAULT '',
            email           VARCHAR(500) NOT NULL DEFAULT '',
            active          ENUM('yes','no') NOT NULL DEFAULT 'no',
            schedule        VARCHAR(64)  NOT NULL DEFAULT 'weekly_friday',
            filter_words    VARCHAR(500) NOT NULL DEFAULT '',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ad_account (ad_account_id)
        )
    """)
    print("Table clients: ready")

    # Migration: add filter_words column if missing
    try:
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'fb_receipts' AND TABLE_NAME = 'clients' "
            "AND COLUMN_NAME = 'filter_words'"
        )
        if cur.fetchone()[0] == 0:
            cur.execute("ALTER TABLE clients ADD COLUMN filter_words VARCHAR(500) NOT NULL DEFAULT '' AFTER schedule")
            print("Migration: added filter_words column to clients")
    except Exception as e:
        print(f"Migration check (filter_words): {e}")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            setting_key     VARCHAR(64) PRIMARY KEY,
            setting_value   VARCHAR(500) NOT NULL DEFAULT ''
        )
    """)
    cur.execute("""
        INSERT IGNORE INTO settings (setting_key, setting_value) VALUES
            ('admin_email',      ''),
            ('notify_email',     ''),
            ('schedule_time',    '09:00'),
            ('default_schedule', 'weekly_friday')
    """)
    print("Table settings: ready")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_receipts (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            ad_account_id   VARCHAR(64)   NOT NULL,
            transaction_id  VARCHAR(100)  NOT NULL DEFAULT '',
            gmail_message_id VARCHAR(100) NOT NULL DEFAULT '',
            receipt_for     VARCHAR(255)  NOT NULL DEFAULT '',
            amount          DECIMAL(12,2) NOT NULL DEFAULT 0,
            currency        VARCHAR(10)   NOT NULL DEFAULT 'USD',
            invoice_date    DATETIME      NULL,
            date_range_start VARCHAR(100) NOT NULL DEFAULT '',
            date_range_end  VARCHAR(100)  NOT NULL DEFAULT '',
            payment_method  VARCHAR(100)  NOT NULL DEFAULT '',
            reference_number VARCHAR(100) NOT NULL DEFAULT '',
            billing_reason  VARCHAR(255)  NOT NULL DEFAULT '',
            product_type    VARCHAR(64)   NOT NULL DEFAULT 'Meta ads',
            email_subject   VARCHAR(500)  NOT NULL DEFAULT '',
            pdf_data        LONGBLOB      NULL,
            pdf_filename    VARCHAR(255)  NOT NULL DEFAULT '',
            ad_images_json  LONGTEXT      NULL,
            sent_to         VARCHAR(500)  NOT NULL DEFAULT '',
            sent_at         DATETIME      NULL,
            status          ENUM('sent','failed','pending') NOT NULL DEFAULT 'pending',
            error           VARCHAR(500)  NOT NULL DEFAULT '',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_txn (ad_account_id, transaction_id),
            INDEX idx_account (ad_account_id),
            INDEX idx_status (status)
        )
    """)
    print("Table sent_receipts: ready")

    # Migration: add ad_images_json column if missing
    try:
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'fb_receipts' AND TABLE_NAME = 'sent_receipts' "
            "AND COLUMN_NAME = 'ad_images_json'"
        )
        if cur.fetchone()[0] == 0:
            cur.execute("ALTER TABLE sent_receipts ADD COLUMN ad_images_json LONGTEXT NULL AFTER pdf_filename")
            print("Migration: added ad_images_json column")
    except Exception as e:
        print(f"Migration check: {e}")

conn.commit()
conn.close()
print("\nDone. Run this once — it's safe to re-run (all statements are idempotent).")
