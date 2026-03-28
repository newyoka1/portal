"""
One-time setup: creates the fb_receipts database and tables on Aiven MySQL.
Run once from the facebook-receipt-automation directory:

    python setup_fb_db.py
"""

from pathlib import Path
from dotenv import load_dotenv
import os, pymysql

load_dotenv(Path(__file__).parent / ".env")

host     = os.environ["FB_AIVEN_HOST"]
user     = os.environ["FB_AIVEN_USER"]
password = os.environ["FB_AIVEN_PASSWORD"]
port     = int(os.environ.get("FB_AIVEN_PORT", 3306))
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
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ad_account (ad_account_id)
        )
    """)
    print("Table clients: ready")

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

conn.commit()
conn.close()
print("\nDone. Run this once — it's safe to re-run (all statements are idempotent).")
