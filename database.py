"""Database setup — MySQL (Aiven) via SQLAlchemy + PyMySQL."""
import base64
import logging
import os
import ssl as _ssl_mod
import tempfile
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

_raw_url = os.getenv("DATABASE_URL")

# Construct DATABASE_URL from individual MYSQL_* vars if not set directly.
# This lets all portal sub-apps share one set of credential env vars.
if not _raw_url:
    _host = os.getenv("MYSQL_HOST")
    _port = os.getenv("MYSQL_PORT", "3306")
    _user = os.getenv("MYSQL_USER")
    _pw   = os.getenv("MYSQL_PASSWORD")
    _db   = os.getenv("EMAIL_APPROVAL_DB", "email_approval")
    if _host and _user and _pw:
        _raw_url = f"mysql+pymysql://{_user}:{_pw}@{_host}:{_port}/{_db}"
    else:
        raise RuntimeError(
            "No database config found. Set DATABASE_URL or MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD."
        )

# PyMySQL doesn't read ssl_ca from the query string — strip it from the URL
# and pass it through connect_args instead.
_parsed   = urlparse(_raw_url)
_qs       = parse_qs(_parsed.query)
_ssl_ca   = _qs.pop("ssl_ca",  [None])[0]
_ssl_cert = _qs.pop("ssl_cert",[None])[0]
_ssl_key  = _qs.pop("ssl_key", [None])[0]

# Rebuild the URL without SSL query params
_clean_url = urlunparse(_parsed._replace(query=urlencode({k: v[0] for k, v in _qs.items()})))

# Railway / production: SSL cert supplied as env var content instead of a file path.
# Accepts either:
#   MYSQL_SSL_CA_B64     — base64-encoded PEM (recommended: avoids line-ending corruption)
#   MYSQL_SSL_CA_CONTENT — raw PEM text (works locally, may corrupt in Railway UI)
_ssl_ca_b64     = os.getenv("MYSQL_SSL_CA_B64")
_ssl_ca_content = os.getenv("MYSQL_SSL_CA_CONTENT")

if not _ssl_ca and (_ssl_ca_b64 or _ssl_ca_content):
    try:
        if _ssl_ca_b64:
            pem_bytes = base64.b64decode(_ssl_ca_b64).replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        else:
            pem_bytes = _ssl_ca_content.replace("\r\n", "\n").replace("\r", "\n").encode()
        _tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="wb")
        _tmp.write(pem_bytes)
        _tmp.close()
        # Validate before using — bad cert content causes a crash at connect time
        _ssl_mod.create_default_context(cafile=_tmp.name)
        _ssl_ca = _tmp.name
    except Exception as _e:
        logging.warning("SSL CA cert env var is invalid (%s) — connecting without CA verification", _e)

_ssl_args: dict = {}
if _ssl_ca:
    _ssl_args["ca"] = _ssl_ca
if _ssl_cert:
    _ssl_args["cert"] = _ssl_cert
if _ssl_key:
    _ssl_args["key"] = _ssl_key

# Always pass ssl={...} so the connection is encrypted.
# If no CA cert is configured, PyMySQL treats an empty dict as
# "SSL on, no cert verification" — still encrypted, no cafile needed.
_connect_ssl = _ssl_args if _ssl_args else {}

engine = create_engine(
    _clean_url,
    connect_args={"ssl": _connect_ssl},
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
