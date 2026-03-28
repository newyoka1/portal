"""
Detect which system sent an email (HubSpot, Mailchimp, Constant Contact)
and extract a clean HTML body for rendering.
"""
import re
from email.message import Message
from models import OriginSystem


# Header / body fingerprints per system
_FINGERPRINTS: list[tuple[str, list[str]]] = [
    (OriginSystem.hubspot, [
        "X-HubSpot",
        "hubspot.net",
        "hs-email",
        "tracking.hubspot",
    ]),
    (OriginSystem.mailchimp, [
        "X-Mailer: MailChimp",
        "list-unsubscribe.*mailchimp",
        "mc.sendgrid",
        "mailchimp.com",
        "mc_eid",
    ]),
    (OriginSystem.constant_contact, [
        "constantcontact.com",
        "X-Mailer: Roving",
        "campaign-archive.com",
    ]),
]


def detect_origin(raw_headers: str, html_body: str) -> str:
    combined = (raw_headers + "\n" + html_body).lower()
    for system, patterns in _FINGERPRINTS:
        for pat in patterns:
            if re.search(pat.lower(), combined):
                return system
    return OriginSystem.unknown


def extract_html_body(msg: Message) -> str:
    """Walk the MIME tree and return the first text/html part."""
    if msg.get_content_type() == "text/html":
        payload = msg.get_payload(decode=True)
        charset = msg.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace") if payload else ""

    if msg.is_multipart():
        # Prefer HTML over plain text
        html_part = None
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html" and html_part is None:
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"
                html_part = payload.decode(charset, errors="replace") if payload else ""
        if html_part:
            return html_part

    # Fall back to plain text wrapped in a pre tag
    payload = msg.get_payload(decode=True)
    if payload:
        charset = msg.get_content_charset() or "utf-8"
        text = payload.decode(charset, errors="replace")
        return f"<pre style='font-family:sans-serif;white-space:pre-wrap'>{text}</pre>"

    return "<p><em>(No content)</em></p>"


def extract_text_body(msg: Message) -> str:
    """Return the plain-text part if present."""
    for part in msg.walk():
        if part.get_content_type() == "text/plain":
            payload = part.get_payload(decode=True)
            charset = part.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace") if payload else ""
    return ""


def get_raw_headers(msg: Message) -> str:
    """Serialize all headers as a single string for fingerprinting."""
    return "\n".join(f"{k}: {v}" for k, v in msg.items())
