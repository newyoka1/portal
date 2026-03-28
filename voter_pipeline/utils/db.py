"""
utils/db.py - shared MySQL connection helper
=============================================
All scripts should import from here instead of hardcoding credentials.

Usage:
    from utils.db import get_conn
    conn = get_conn()               # defaults to nys_voter_tagging
    conn = get_conn('boe_donors')   # specify database

    from utils.db import get_aiven_conn
    conn = get_aiven_conn()         # connects to Aiven remote DB
"""
import os
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

# ── Local MySQL ────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv('DB_HOST') or os.getenv('MYSQL_HOST', 'localhost')
DB_USER     = os.getenv('DB_USER') or os.getenv('MYSQL_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD') or os.getenv('MYSQL_PASSWORD', '')
DB_PORT     = int(os.getenv('DB_PORT') or os.getenv('MYSQL_PORT', '3306'))

# ── Aiven Remote MySQL ─────────────────────────────────────────────────────────
# Falls back to MYSQL_* vars so all portal sub-apps can share one set of creds.
AIVEN_HOST     = os.getenv('AIVEN_HOST') or os.getenv('MYSQL_HOST', '')
AIVEN_USER     = os.getenv('AIVEN_USER') or os.getenv('MYSQL_USER', 'avnadmin')
AIVEN_PASSWORD = os.getenv('AIVEN_PASSWORD') or os.getenv('MYSQL_PASSWORD', '')
AIVEN_PORT     = int(os.getenv('AIVEN_PORT') or os.getenv('MYSQL_PORT', '3306'))
AIVEN_DB       = os.getenv('AIVEN_DB', 'nys_voter_tagging')
AIVEN_SSL_CA   = os.getenv('AIVEN_SSL_CA', '')


def get_conn(database='nys_voter_tagging', autocommit=False, timeout=7200, local_infile=False):
    """Return a pymysql connection to the LOCAL database.

    Parameters
    ----------
    database    : target schema (default: nys_voter_tagging)
    autocommit  : set True for scripts that do not manage transactions manually
    timeout     : connect / read / write timeout in seconds
    local_infile: set True when using LOAD DATA LOCAL INFILE
    """
    kwargs = dict(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        database=database,
        charset='utf8mb4',
        connect_timeout=timeout,
        read_timeout=timeout,
        write_timeout=timeout,
        autocommit=autocommit,
    )
    if local_infile:
        from pymysql.constants import CLIENT
        kwargs['local_infile'] = True
        kwargs['client_flag'] = CLIENT.LOCAL_FILES
    return pymysql.connect(**kwargs)


_UNSET = object()   # sentinel — distinguishes "not passed" from explicit None

def get_aiven_conn(database=_UNSET, autocommit=False, timeout=600):
    """Return a pymysql connection to Aiven remote MySQL.
    Requires AIVEN_HOST, AIVEN_USER, AIVEN_PASSWORD in .env
    SSL is required by Aiven — set AIVEN_SSL_CA to path of ca.pem.

    Pass database=None to connect without selecting a schema (for CREATE DATABASE).
    """
    if not AIVEN_HOST:
        raise ValueError("AIVEN_HOST not set in .env — add your Aiven credentials.")
    if not AIVEN_PASSWORD:
        raise ValueError("AIVEN_PASSWORD not set in .env")

    db = AIVEN_DB if database is _UNSET else database

    ssl_config = None
    if AIVEN_SSL_CA and os.path.exists(AIVEN_SSL_CA):
        ssl_config = {'ca': AIVEN_SSL_CA, 'check_hostname': False}
    else:
        ssl_config = {'check_hostname': False}

    kwargs = dict(
        host=AIVEN_HOST,
        user=AIVEN_USER,
        password=AIVEN_PASSWORD,
        port=AIVEN_PORT,
        charset='utf8mb4',
        connect_timeout=timeout,
        read_timeout=timeout,
        write_timeout=timeout,
        autocommit=autocommit,
        ssl=ssl_config,
    )
    if db is not None:
        kwargs['database'] = db

    return pymysql.connect(**kwargs)
