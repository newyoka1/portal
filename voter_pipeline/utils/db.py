"""
utils/db.py - shared MySQL connection helper
=============================================
All scripts should import from here instead of hardcoding credentials.

Usage:
    from utils.db import get_conn
    conn = get_conn()               # defaults to nys_voter_tagging
    conn = get_conn('boe_donors')   # specify database
"""
import os
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

# ── MySQL (VPS local) ──────────────────────────────────────────────────────────
DB_HOST     = os.getenv('DB_HOST') or os.getenv('MYSQL_HOST', 'localhost')
DB_USER     = os.getenv('DB_USER') or os.getenv('MYSQL_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD') or os.getenv('MYSQL_PASSWORD', '')
DB_PORT     = int(os.getenv('DB_PORT') or os.getenv('MYSQL_PORT', '3306'))


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


