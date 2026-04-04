"""Lightweight audit-trail helper — logs state changes to the audit_logs table."""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def log_action(db, *, email_id=None, user_id=None, actor_name="", action="", detail=""):
    """Record an auditable event.  Imports are deferred to avoid circular deps."""
    from models import AuditLog
    try:
        db.add(AuditLog(
            email_id=email_id,
            user_id=user_id,
            actor_name=actor_name,
            action=action,
            detail=detail[:1000],
        ))
    except Exception as exc:
        logger.warning("Audit log failed: %s", exc)
