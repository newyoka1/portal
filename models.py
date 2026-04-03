"""SQLAlchemy ORM models."""
from datetime import datetime, timezone
from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, String, Text, Enum
)
from sqlalchemy.orm import relationship
import enum

from database import Base


class EmailStatus(str, enum.Enum):
    pending   = "pending"     # in queue, not yet assigned
    in_review = "in_review"   # assigned to client, awaiting approvals
    approved  = "approved"    # all required approvers signed off
    rejected  = "rejected"    # at least one required approver rejected


class ApprovalDecision(str, enum.Enum):
    pending  = "pending"
    approved = "approved"
    rejected = "rejected"


class OriginSystem(str, enum.Enum):
    hubspot          = "HubSpot"
    mailchimp        = "Mailchimp"
    constant_contact = "Constant Contact"
    unknown          = "Unknown"


# ---------------------------------------------------------------------------
# Users (staff / approvers)
# ---------------------------------------------------------------------------
class User(Base):
    __tablename__ = "users"

    id            = Column(Integer, primary_key=True, index=True)
    name          = Column(String(120), nullable=False)
    email         = Column(String(200), unique=True, nullable=False, index=True)
    password_hash = Column(String(200), nullable=False)
    is_admin      = Column(Boolean, default=False)
    voter_role    = Column(String(20), nullable=True, default=None)
    # voter_role: None = no pipeline access, "full" = all tabs, "export_viewer" = export+status+issues
    created_at    = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    approvals     = relationship("Approval", back_populates="user")
    comments      = relationship("Comment",  back_populates="user")
    client_roles  = relationship("ClientApprover", back_populates="user")


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
class Client(Base):
    __tablename__ = "clients"

    id             = Column(Integer, primary_key=True, index=True)
    name           = Column(String(200), nullable=False)
    slug           = Column(String(100), unique=True, index=True)
    from_email     = Column(String(200), nullable=True, default=None)  # per-client sender address
    subject_filter = Column(String(200), nullable=True, default=None)  # per-client subject filter word
    email_template = Column(Text, nullable=True, default=None)         # custom approval email HTML body
    sms_template   = Column(Text, nullable=True, default=None)         # custom approval SMS text
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    emails       = relationship("Email", back_populates="client")
    approvers    = relationship("ClientApprover", back_populates="client", cascade="all, delete-orphan")
    integrations = relationship("ClientIntegration", back_populates="client", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# Per-client platform integrations
# ---------------------------------------------------------------------------
class ClientIntegration(Base):
    __tablename__ = "client_integrations"

    id             = Column(Integer, primary_key=True, index=True)
    client_id      = Column(Integer, ForeignKey("clients.id"), nullable=False)
    platform       = Column(String(50), nullable=False)   # hubspot | mailchimp | campaign_monitor
    api_key        = Column(String(500), nullable=False)
    # Platform-specific extras stored as JSON string:
    #   HubSpot        — no extras needed
    #   Mailchimp      — {"data_center": "us1"}  (derived from api_key suffix, stored for speed)
    #   Campaign Monitor — {"cm_client_id": "abc123"}
    extra_config   = Column(Text, default="{}")
    enabled        = Column(Boolean, default=True)
    last_synced_at = Column(DateTime, nullable=True)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    client = relationship("Client", back_populates="integrations")


# ---------------------------------------------------------------------------
# Per-client approver configuration
# ---------------------------------------------------------------------------
class ClientApprover(Base):
    __tablename__ = "client_approvers"

    id        = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    user_id   = Column(Integer, ForeignKey("users.id"),   nullable=True)   # nullable for external approvers
    approver_name  = Column(String(200), nullable=True)   # external approver name
    approver_email = Column(String(200), nullable=True)   # external approver email
    approver_phone = Column(String(30),  nullable=True)   # phone for SMS approval links
    required  = Column(Boolean, default=True)   # True = must approve, False = optional

    client    = relationship("Client", back_populates="approvers")
    user      = relationship("User",   back_populates="client_roles")

    @property
    def display_name(self):
        if self.user:
            return self.user.name
        return self.approver_name or self.approver_email or "Unknown"

    @property
    def email(self):
        if self.user:
            return self.user.email
        return self.approver_email or ""


# ---------------------------------------------------------------------------
# Ingested emails
# ---------------------------------------------------------------------------
class Email(Base):
    __tablename__ = "emails"

    id            = Column(Integer, primary_key=True, index=True)
    client_id     = Column(Integer, ForeignKey("clients.id"), nullable=True)  # null until assigned
    gmail_message_id = Column(String(200), unique=True, index=True)           # prevents re-ingestion
    subject       = Column(String(500), nullable=False)
    from_address  = Column(String(200), nullable=False)
    from_name     = Column(String(200), default="")
    html_body     = Column(Text, default="")
    text_body     = Column(Text, default="")
    origin_system = Column(String(50), default=OriginSystem.unknown)
    received_at          = Column(DateTime, nullable=False)
    status               = Column(String(20), default=EmailStatus.pending)
    assigned_at          = Column(DateTime, nullable=True)
    sent_for_approval_at = Column(DateTime, nullable=True)

    client        = relationship("Client",   back_populates="emails")
    approvals     = relationship("Approval", back_populates="email", cascade="all, delete-orphan")
    comments      = relationship("Comment",  back_populates="email", cascade="all, delete-orphan",
                                 order_by="Comment.created_at")


# ---------------------------------------------------------------------------
# Approvals (one row per approver per email)
# ---------------------------------------------------------------------------
class Approval(Base):
    __tablename__ = "approvals"

    id              = Column(Integer, primary_key=True, index=True)
    email_id        = Column(Integer, ForeignKey("emails.id"), nullable=False)
    user_id         = Column(Integer, ForeignKey("users.id"),  nullable=True)   # nullable for external
    approver_name   = Column(String(200), nullable=True)
    approver_email  = Column(String(200), nullable=True)
    approver_phone  = Column(String(30),  nullable=True)
    required        = Column(Boolean, default=True)
    decision        = Column(String(20), default=ApprovalDecision.pending)
    note            = Column(Text, default="")
    decided_at      = Column(DateTime, nullable=True)
    token           = Column(String(100), unique=True, nullable=True, index=True)

    email      = relationship("Email", back_populates="approvals")
    user       = relationship("User",  back_populates="approvals")

    @property
    def display_name(self):
        if self.user:
            return self.user.name
        return self.approver_name or self.approver_email or "Unknown"

    @property
    def display_email(self):
        if self.user:
            return self.user.email
        return self.approver_email or ""


# ---------------------------------------------------------------------------
# Comments (flat thread per email)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Portal Settings (key-value config, editable in /settings admin page)
# ---------------------------------------------------------------------------
class PortalSetting(Base):
    __tablename__ = "portal_settings"

    key       = Column(String(100), primary_key=True)
    value     = Column(Text, default="")
    label     = Column(String(200), default="")
    category  = Column(String(50), default="general")
    is_secret = Column(Boolean, default=False)


# ---------------------------------------------------------------------------
# Comments (flat thread per email)
# ---------------------------------------------------------------------------
class Comment(Base):
    __tablename__ = "comments"

    id             = Column(Integer, primary_key=True, index=True)
    email_id       = Column(Integer, ForeignKey("emails.id"), nullable=False)
    user_id        = Column(Integer, ForeignKey("users.id"),  nullable=True)   # nullable for external approvers
    commenter_name = Column(String(200), nullable=True)   # display name for external approvers
    body           = Column(Text, nullable=False)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    parent_id      = Column(Integer, ForeignKey("comments.id"), nullable=True)

    email      = relationship("Email",   back_populates="comments")
    user       = relationship("User",    back_populates="comments")
    replies    = relationship("Comment", backref="parent", remote_side=[id])
