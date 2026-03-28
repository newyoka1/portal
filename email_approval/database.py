"""Database setup — MySQL (Aiven) via SQLAlchemy + PyMySQL."""
import os
import tempfile
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

_raw_url = os.getenv("DATABASE_URL")
if not _raw_url:
    raise RuntimeError(
        "DATABASE_URL is not set. Copy .env.example to .env and fill in your Aiven MySQL credentials."
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
# Write it to a temp file so PyMySQL can read it.
_ssl_ca_content = os.getenv("MYSQL_SSL_CA_CONTENT")
if not _ssl_ca and _ssl_ca_content:
    _tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="w")
    _tmp.write(_ssl_ca_content)
    _tmp.close()
    _ssl_ca = _tmp.name

_ssl_args: dict = {}
if _ssl_ca:
    _ssl_args["ca"] = _ssl_ca
if _ssl_cert:
    _ssl_args["cert"] = _ssl_cert
if _ssl_key:
    _ssl_args["key"] = _ssl_key

engine = create_engine(
    _clean_url,
    connect_args={"ssl": _ssl_args} if _ssl_args else {},
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
