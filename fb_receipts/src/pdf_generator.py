"""
PDF receipt generator — matches Meta's TCPDF billing receipt layout.

Layout (A4 page):

  Receipt for [Client Name]                         ∞ Meta   (logo top-right)
  Account ID: XXXXXXXXX
  ─────────────────────────── thin line ─────────────────────

  Invoice/Payment Date                               (label, ~8pt, gray)
  Jan 29, 2026, 8:30 PM                             (value, ~10pt, bold black)

  Payment method                                     (label)
  Paypal account george@politikanyc.com              (value, bold)
                                                      Paid         (right, gray ~12pt)
  Transaction ID                                     (label)
  25840962155592868-25958827573806318                  $20.00       (right, black ~28pt)

  Product Type                                        You're being billed because ...
  Meta ads                                           (right, gray ~8pt)

  ─────────────────────── thin gray line ─────────────────────

  Campaigns                                          (section heading, ~12pt bold)

  Campaign Name                                      $XX.XX
  From [date] to [date]
  · · · · · · · · · · · · · orange dashed line · · ·
      AdSet Name                 X,XXX Impressions   $XX.XX

  ─────────────────────── thin gray line ─────────────────────

  Meta Platforms, Inc.                        [Client Name]  (right-aligned)
  1 Meta Way                              [Client Address]
  Menlo Park, CA 94025
  United States

  Powered by TCPDF (www.tcpdf.org)
"""

import logging
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    HRFlowable, Image,
)

from src.config import RECEIPT_DOWNLOAD_DIR

# Meta logo — resolved relative to this file so it works from any working directory
_META_LOGO_PATH = Path(__file__).resolve().parent.parent.parent.parent / "static" / "meta-logo.png"

logger = logging.getLogger(__name__)

# ── Colors (sampled from real Meta receipt PDF) ──────────────────────────────
C_TITLE      = colors.HexColor("#1C2B33")   # dark blue-black for title
C_BLACK      = colors.HexColor("#1C1E21")   # body text / values
C_GRAY       = colors.HexColor("#65676B")   # labels, secondary text
C_LIGHT_GRAY = colors.HexColor("#DADDE1")   # thin separator lines
C_ORANGE     = colors.HexColor("#E8913A")   # dashed campaign separator
C_META_BLUE  = colors.HexColor("#0668E1")   # Meta logo color
C_FOOTER     = colors.HexColor("#8A8D91")   # footer text
C_TCPDF      = colors.HexColor("#C8CCD0")   # "Powered by TCPDF" text

# ── Styles ───────────────────────────────────────────────────────────────────
def _s(name, **kw):
    d = {"fontName": "Helvetica", "textColor": C_BLACK, "fontSize": 10, "leading": 13}
    d.update(kw)
    return ParagraphStyle(name, **d)

S_TITLE      = _s("T",  fontSize=14, textColor=C_TITLE, leading=18)
S_ACCT_ID    = _s("A",  fontSize=9,  textColor=C_GRAY, leading=12)
S_LABEL      = _s("L",  fontSize=8,  textColor=C_GRAY, leading=11, spaceBefore=6)
S_VALUE      = _s("V",  fontSize=10, fontName="Helvetica-Bold", textColor=C_BLACK, leading=13, spaceAfter=1)
S_PAID_LABEL = _s("PL", fontSize=12, textColor=C_GRAY, alignment=2, leading=15)
S_PAID_AMT   = _s("PA", fontSize=28, fontName="Helvetica", textColor=C_BLACK, alignment=2, leading=34)
S_PAID_NOTE  = _s("PN", fontSize=8,  textColor=C_GRAY, alignment=2, leading=11)
S_SECTION    = _s("S",  fontSize=12, fontName="Helvetica-Bold", textColor=C_BLACK, spaceBefore=10, spaceAfter=6, leading=16)
S_CAMP       = _s("C",  fontSize=10, fontName="Helvetica-Bold", textColor=C_BLACK, leading=13)
S_CAMP_SPEND = _s("CS", fontSize=10, textColor=C_BLACK, alignment=2, leading=13)
S_CAMP_DATE  = _s("CD", fontSize=8,  textColor=C_GRAY, leading=11)
S_ADSET_NAME = _s("AN", fontSize=8,  textColor=C_GRAY, leading=11)
S_ADSET_IMPR = _s("AI", fontSize=8,  textColor=C_GRAY, alignment=1, leading=11)
S_ADSET_SPEND= _s("AS", fontSize=8,  textColor=C_BLACK, alignment=2, leading=11)
S_FOOTER     = _s("F",  fontSize=7,  textColor=C_FOOTER, leading=10)
S_FOOTER_R   = _s("FR", fontSize=7,  textColor=C_FOOTER, alignment=2, leading=10)
S_TCPDF      = _s("TC", fontSize=6,  textColor=C_TCPDF, alignment=1, leading=8)
# Meta logo text style
S_META_LOGO  = _s("ML", fontSize=16, fontName="Helvetica-Bold", textColor=C_META_BLUE, alignment=2, leading=20)


def _build_header(story, client_name: str, account_id: str, W: float):
    """Title + Account ID + Meta logo + separator line."""
    # Meta logo image — height fixed at 18pt, width scales proportionally
    if _META_LOGO_PATH.exists():
        logo_cell = Image(str(_META_LOGO_PATH), height=18, width=18 * (1280 / 258))
    else:
        # Fallback to text if image not found
        logo_cell = Paragraph("\u221e Meta", S_META_LOGO)

    # Title row: "Receipt for ..." left, Meta logo right
    title_table = Table([[
        [Paragraph(f"Receipt for {client_name}", S_TITLE),
         Paragraph(f"Account ID: {account_id}", S_ACCT_ID)],
        logo_cell,
    ]], colWidths=[W * 0.65, W * 0.35])
    title_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(title_table)
    story.append(Spacer(1, 2 * mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=C_LIGHT_GRAY))
    story.append(Spacer(1, 6 * mm))


def _build_info_section(
    story, W: float, date_str: str, txn_id: str, amount: float,
    payment_method: str = "", billing_reason: str = "",
    reference_number: str = "",
):
    """Invoice date, payment method, transaction ID, paid amount."""
    # Invoice/Payment Date
    story.append(Paragraph("Invoice/Payment Date", S_LABEL))
    story.append(Paragraph(date_str, S_VALUE))

    # Payment method (if available)
    if payment_method:
        story.append(Paragraph("Payment method", S_LABEL))
        story.append(Paragraph(payment_method, S_VALUE))
        if reference_number:
            story.append(Paragraph(f"Reference Number: {reference_number}", S_VALUE))

    # Transaction ID (left) + Paid amount (right)
    story.append(Spacer(1, 1 * mm))
    left_col = [
        Paragraph("Transaction ID", S_LABEL),
        Paragraph(txn_id, S_VALUE),
        Spacer(1, 2 * mm),
        Paragraph("Product Type", S_LABEL),
        Paragraph("<b>Meta ads</b>", _s("VB", fontSize=10, fontName="Helvetica-Bold",
                                          textColor=C_BLACK, leading=13)),
    ]
    right_col = [
        Paragraph("Paid", S_PAID_LABEL),
        Paragraph(f"${amount:,.2f}", S_PAID_AMT),
    ]
    if billing_reason:
        right_col.append(Paragraph(billing_reason, S_PAID_NOTE))

    info_table = Table([[left_col, right_col]], colWidths=[W * 0.55, W * 0.45])
    info_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(info_table)

    story.append(Spacer(1, 5 * mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=C_LIGHT_GRAY))


def _build_campaigns(story, W: float, campaigns: list[dict], adsets: list[dict], amount: float):
    """Campaigns section with adset breakdown."""
    story.append(Paragraph("Campaigns", S_SECTION))

    adsets_by_campaign = {}
    for a in adsets:
        cid = a.get("campaign_id", "")
        adsets_by_campaign.setdefault(cid, []).append(a)

    if not campaigns:
        story.append(Paragraph(f"Total ad spend: ${amount:,.2f}", S_VALUE))
        return

    for c in sorted(campaigns, key=lambda x: -float(x.get("spend", 0))):
        camp_name = c.get("campaign_name", "Campaign")
        camp_spend = float(c.get("spend", 0))
        camp_id = c.get("campaign_id", "")
        camp_start = c.get("date_start", "")
        camp_end = c.get("date_stop", "")

        # Format dates like Meta: "From Jan 29, 2026, 12:00 AM to Jan 29, 2026, 8:30 PM"
        try:
            ds = datetime.strptime(camp_start, "%Y-%m-%d").strftime("%b %d, %Y")
            de = datetime.strptime(camp_end, "%Y-%m-%d").strftime("%b %d, %Y")
            date_range = f"From {ds}, 12:00 AM to {de}, 11:59 PM"
        except Exception:
            date_range = f"From {camp_start} to {camp_end}" if camp_start else ""

        # Campaign name + spend on same line
        ct = Table([[
            Paragraph(f"<b>{camp_name}</b>", S_CAMP),
            Paragraph(f"${camp_spend:,.2f}", S_CAMP_SPEND),
        ]], colWidths=[W * 0.75, W * 0.25])
        ct.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(ct)

        if date_range:
            story.append(Paragraph(date_range, S_CAMP_DATE))

        # Orange dashed separator
        story.append(Spacer(1, 1.5 * mm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=C_ORANGE, dash=[2, 2]))
        story.append(Spacer(1, 1.5 * mm))

        # Ad sets under this campaign
        camp_adsets = adsets_by_campaign.get(camp_id, [])
        if camp_adsets:
            for a in camp_adsets:
                adset_name = a.get("adset_name", "Ad Set")
                if len(adset_name) > 60:
                    adset_name = adset_name[:57] + "..."
                adset_impr = int(a.get("impressions", 0) or 0)
                adset_spend = float(a.get("spend", 0))

                at = Table([[
                    Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{adset_name}", S_ADSET_NAME),
                    Paragraph(f"{adset_impr:,} Impressions", S_ADSET_IMPR),
                    Paragraph(f"${adset_spend:,.2f}", S_ADSET_SPEND),
                ]], colWidths=[W * 0.50, W * 0.28, W * 0.22])
                at.setStyle(TableStyle([
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 1),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                ]))
                story.append(at)
        else:
            # No adset detail — show impressions at campaign level
            camp_impr = int(c.get("impressions", 0) or 0)
            if camp_impr:
                it = Table([[
                    Paragraph("", S_ADSET_NAME),
                    Paragraph(f"{camp_impr:,} Impressions", S_ADSET_IMPR),
                    Paragraph(f"${camp_spend:,.2f}", S_ADSET_SPEND),
                ]], colWidths=[W * 0.50, W * 0.28, W * 0.22])
                it.setStyle(TableStyle([
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ]))
                story.append(it)

        story.append(Spacer(1, 3 * mm))


def _build_footer(story, W: float, client_name: str, client_address: str = ""):
    """Meta address (left) + client address (right-aligned)."""
    story.append(Spacer(1, 8 * mm))

    meta_lines = [
        "Meta Platforms, Inc.",
        "1 Meta Way",
        "Menlo Park, CA 94025",
        "United States",
    ]
    client_lines = [f"<b>{client_name}</b>"]
    if client_address:
        client_lines.extend(line.strip() for line in client_address.split("\n") if line.strip())

    # Pad to same length
    max_len = max(len(meta_lines), len(client_lines))
    while len(meta_lines) < max_len:
        meta_lines.append("")
    while len(client_lines) < max_len:
        client_lines.append("")

    footer_rows = [
        [Paragraph(meta_lines[i], S_FOOTER), Paragraph(client_lines[i], S_FOOTER_R)]
        for i in range(max_len)
    ]
    ft = Table(footer_rows, colWidths=[W * 0.5, W * 0.5])
    ft.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(ft)

    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph("Powered by TCPDF (www.tcpdf.org)", S_TCPDF))


def _make_doc(filepath: Path):
    return SimpleDocTemplate(
        str(filepath), pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=12 * mm, bottomMargin=12 * mm,
    )


def generate_transaction_pdf(
    client_name: str,
    ad_account_id: str,
    transaction: dict,
    campaigns: list[dict],
    adsets: list[dict],
    base_dir: Path | None = None,
    client_address: str = "",
) -> Path | None:
    """Generate one Meta-style receipt PDF for a single billing event."""
    safe_account = ad_account_id.replace("act_", "")
    root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
    dest = root / safe_account
    dest.mkdir(parents=True, exist_ok=True)

    txn_id = transaction.get("id", "unknown")
    amount = float(transaction.get("amount", 0))
    txn_time = transaction.get("time", "")
    billing_reason = transaction.get("billing_reason", "")
    payment_method = transaction.get("payment_method", "")

    try:
        dt = datetime.fromisoformat(txn_time.replace("Z", "+00:00").replace("+0000", "+00:00"))
        date_str = dt.strftime("%b %d, %Y, %I:%M %p").replace(" 0", " ")
        date_file = dt.strftime("%Y%m%d_%H%M")
    except Exception:
        date_str = txn_time[:19] if txn_time else "Unknown"
        date_file = "unknown"

    filepath = dest / f"receipt_{safe_account}_{date_file}.pdf"

    try:
        doc = _make_doc(filepath)
        W = A4[0] - 30 * mm
        story = []

        _build_header(story, client_name, safe_account, W)
        _build_info_section(
            story, W, date_str, txn_id, amount,
            payment_method=payment_method,
            billing_reason=billing_reason,
        )
        _build_campaigns(story, W, campaigns, adsets, amount)
        _build_footer(story, W, client_name, client_address)

        doc.build(story)
        logger.info("Generated receipt PDF: %s", filepath)
        return filepath

    except Exception as e:
        logger.error("Failed to generate PDF for txn %s: %s", txn_id, e)
        return None


def generate_receipt_pdf(
    client_name: str,
    ad_account_id: str,
    receipts: list[dict],
    start_date: datetime,
    end_date: datetime,
    base_dir: Path | None = None,
    ad_images: list[Path] | None = None,
    campaigns: list[dict] | None = None,
) -> Path | None:
    """Legacy wrapper — builds a transaction from daily spend and delegates."""
    if not receipts:
        return None
    safe_account = ad_account_id.replace("act_", "")
    total_spend = sum(float(r.get("amount", 0)) for r in receipts)
    period_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    txn = {
        "id": f"{safe_account}-{period_str}",
        "time": end_date.isoformat(),
        "amount": str(total_spend),
        "currency": "USD",
    }
    return generate_transaction_pdf(
        client_name=client_name,
        ad_account_id=ad_account_id,
        transaction=txn,
        campaigns=campaigns or [],
        adsets=[],
        base_dir=base_dir,
    )


def generate_email_receipt_pdf(
    receipt: dict,
    campaigns: list[dict],
    adsets: list[dict],
    ad_images: list[Path] | None = None,
    base_dir: Path | None = None,
) -> Path | None:
    """
    Generate a receipt PDF from parsed Gmail receipt email data.

    Uses the exact fields from the email: receipt_for, account_id, transaction_id,
    amount, date_range, payment_method, billing_reason, etc.
    """
    acct_id = receipt.get("account_id", "")
    root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
    dest = root / acct_id
    dest.mkdir(parents=True, exist_ok=True)

    txn_id = receipt.get("transaction_id", "unknown")
    amount = float(receipt.get("amount", 0))
    client_name = receipt.get("receipt_for", "Client")
    payment_method = receipt.get("payment_method", "")
    reference_number = receipt.get("reference_number", "")
    billing_reason = receipt.get("billing_reason", "")
    email_date = receipt.get("email_date", "")
    client_address = receipt.get("client_address", "")

    try:
        dt = datetime.fromisoformat(email_date)
        date_str = dt.strftime("%b %d, %Y, %I:%M %p").replace(" 0", " ")
        date_file = dt.strftime("%Y%m%d_%H%M")
    except Exception:
        date_str = email_date[:19] if email_date else "Unknown"
        date_file = "unknown"

    filepath = dest / f"receipt_{acct_id}_{date_file}.pdf"

    try:
        doc = _make_doc(filepath)
        W = A4[0] - 30 * mm
        story = []

        _build_header(story, client_name, acct_id, W)
        _build_info_section(
            story, W, date_str, txn_id, amount,
            payment_method=payment_method,
            billing_reason=billing_reason,
            reference_number=reference_number,
        )
        _build_campaigns(story, W, campaigns, adsets, amount)
        _build_footer(story, W, client_name, client_address)

        doc.build(story)
        logger.info("Generated email receipt PDF: %s", filepath)
        return filepath

    except Exception as e:
        logger.error("Failed to generate email receipt PDF: %s", e)
        return None
