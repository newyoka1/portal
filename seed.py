"""
Run once to create the first admin user.
Usage:
  py seed.py
"""
import os
import ssl
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

# ── Step 1: create the database if it doesn't exist ────────────────────────
import pymysql

url = urlparse(os.environ["DATABASE_URL"])
db_name  = url.path.lstrip("/").split("?")[0]
ssl_ca   = parse_qs(url.query).get("ssl_ca", [None])[0]

ssl_opts = {"ca": ssl_ca} if ssl_ca else None

conn = pymysql.connect(
    host=url.hostname,
    port=url.port or 3306,
    user=url.username,
    password=url.password,
    ssl=ssl_opts,
)
with conn.cursor() as cur:
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
conn.close()
print(f"Database `{db_name}` ready.")

# ── Step 2: create all tables via SQLAlchemy ────────────────────────────────
from database import Base, engine, SessionLocal
from models import User       # noqa: F401 — must be imported so Base knows the models
from auth import hash_password

Base.metadata.create_all(bind=engine)
print("Tables created.")

# ── Step 3: create the first admin user ────────────────────────────────────
db = SessionLocal()

name     = input("Admin name:  ").strip()
email    = input("Admin email: ").strip()
password = input("Password:    ").strip()

existing = db.query(User).filter(User.email == email).first()
if existing:
    print(f"User '{email}' already exists — nothing changed.")
else:
    db.add(User(
        name=name,
        email=email,
        password_hash=hash_password(password),
        is_admin=True,
    ))
    db.commit()
    print(f"Admin user '{name}' created successfully.")

db.close()
