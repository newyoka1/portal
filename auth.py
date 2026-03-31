"""Session-based auth helpers — file-backed to survive restarts."""
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import bcrypt
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from database import get_db
from models import User

# File-backed session store — survives single-process restarts.
# Key: token, Value: {"user_id": int, "expires": ISO timestamp}
_sessions: dict[str, dict] = {}
_SESSION_FILE = Path(__file__).parent / ".sessions.json"

SESSION_COOKIE = "ea_session"
SESSION_TTL_HOURS = 24 * 7  # 1 week


def _load_sessions() -> None:
    """Load sessions from disk into memory, discarding expired ones."""
    global _sessions
    if not _SESSION_FILE.exists():
        return
    try:
        data = json.loads(_SESSION_FILE.read_text())
        now = datetime.now(timezone.utc)
        for token, info in data.items():
            expires = datetime.fromisoformat(info["expires"])
            if now < expires:
                _sessions[token] = {"user_id": info["user_id"], "expires": expires}
    except (json.JSONDecodeError, KeyError, ValueError):
        _sessions = {}


def _save_sessions() -> None:
    """Persist current sessions to disk."""
    data = {}
    for token, info in _sessions.items():
        data[token] = {
            "user_id": info["user_id"],
            "expires": info["expires"].isoformat(),
        }
    _SESSION_FILE.write_text(json.dumps(data))


# Load existing sessions on import (i.e. at startup)
_load_sessions()


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = {
        "user_id": user_id,
        "expires": datetime.now(timezone.utc) + timedelta(hours=SESSION_TTL_HOURS),
    }
    _save_sessions()
    return token


def delete_session(token: str) -> None:
    _sessions.pop(token, None)
    _save_sessions()


def purge_expired_sessions() -> int:
    """Remove expired sessions from the store. Returns count removed."""
    now = datetime.now(timezone.utc)
    expired = [k for k, v in _sessions.items() if now > v["expires"]]
    for k in expired:
        del _sessions[k]
    if expired:
        _save_sessions()
    return len(expired)


def _resolve_token(token: Optional[str]) -> Optional[int]:
    if not token or token not in _sessions:
        return None
    session = _sessions[token]
    if datetime.now(timezone.utc) > session["expires"]:
        _sessions.pop(token, None)
        _save_sessions()
        return None
    return session["user_id"]


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
) -> Optional[User]:
    token = request.cookies.get(SESSION_COOKIE)
    user_id = _resolve_token(token)
    if not user_id:
        return None
    return db.query(User).filter(User.id == user_id).first()


def require_user(user: Optional[User] = Depends(get_current_user)) -> User:
    if not user:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )
    return user


def require_admin(user: User = Depends(require_user)) -> User:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user
