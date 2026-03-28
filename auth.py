"""Session-based auth helpers."""
import secrets
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from database import get_db
from models import User

# In-memory session store — fine for a single Railway instance.
# Key: token, Value: {"user_id": int, "expires": datetime}
_sessions: dict[str, dict] = {}

SESSION_COOKIE = "ea_session"
SESSION_TTL_HOURS = 24 * 7  # 1 week


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = {
        "user_id": user_id,
        "expires": datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS),
    }
    return token


def delete_session(token: str) -> None:
    _sessions.pop(token, None)


def _resolve_token(token: Optional[str]) -> Optional[int]:
    if not token or token not in _sessions:
        return None
    session = _sessions[token]
    if datetime.utcnow() > session["expires"]:
        _sessions.pop(token, None)
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
