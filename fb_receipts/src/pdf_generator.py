"""
PDF receipt generator — emulates the real Facebook/Meta billing receipt.

Generates one PDF per billing event (transaction), matching the exact
layout of Meta's Payment Activity download:

  Receipt for [Client Name]
  Account ID: XXXXXXXXX                          ∞ Meta

  Invoice/Payment Date
  Mar 13, 2026, 3:52 PM

  Transaction ID                                    Paid
  XXXXXXX-XXXXXXX                                $42.57

  Product Type
  Meta ads

  Campaigns
  ─────────────────────────────────
  Stop The Socialists                             $42.57
  From Mar 13, 2026 to Mar 13, 2026
  · · · · · · · · · · · · · · · · · · · · · · · ·
    18-65+__Conversions   1,624 Impressions       $42.57

  Meta Platforms, Inc.            [Client Address]
"""

import logging
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    HRFlowable, Image as RLImage,
)

from src.config import RECEIPT_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

# Meta brand colors
META_BLUE      = colors.HexColor("#0668E1")
TEXT_PRIMARY    = colors.HexColor("#1C1E21")
TEXT_SECONDARY  = colors.HexColor("#65676B")
TEXT_LABEL      = colors.HexColor("#0E7EE4")
BORDER          = colors.HexColor("#DADDE1")
ORANGE_DASH     = colors.HexColor("#E8913A")


def _s(name, **kw):
    """Quick paragraph style factory."""
    defaults = {"fontName": "Helvetica", "textColor": TEXT_PRIMARY, "fontSize": 10, "leading": 14}
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)


# Pre-built styles
S_TITLE     = _s("Title", fontSize=16, leading=20)
S_SUBTITLE  = _s("Subtitle", fontSize=9, textColor=TEXT_SECONDARY)
S_LABEL     = _s("Label", fontSize=9, textColor=TEXT_LABEL, spaceBefore=10)
S_VALUE     = _s("Value", fontSize=10, spaceAfter=2)
S_PAID      = _s("Paid", fontSize=12, textColor=TEXT_SECONDARY, alignment=2)
S_AMOUNT    = _s("Amount", fontSize=22, fontName="Helvetica-Bold", alignment=2)
S_SECTION   = _s("Section", fontSize=12, fontName="Helvetica-Bold", spaceBefore=16, spaceAfter=6)
S_CAMP_NAME = _s("CampName", fontSize=10, fontName="Helvetica-Bold")
S_CAMP_DATE = _s("CampDate", fontSize=8, textColor=TEXT_SECONDARY)
S_ADSET     = _s("AdSet", fontSize=8, textColor=TEXT_SECONDARY)
S_FOOTER    = _s("Footer", fontSize=8, textColor=TEXT_SECONDARY, leading=11)
S_META_LOGO = _s("MetaLogo", fontSize=18, fontName="Helvetica-Bold", textColor=META_BLUE, alignment=2)


def generate_transaction_pdf(
    client_name: str,
    ad_account_id: str,
    transaction: dict,
    campaigns: list[dict],
    adsets: list[dict],
    base_dir: Path | None = None,
) -> Path | None:
    """
    Generate one Meta-style receipt PDF for a single billing transaction.

    Args:
        client_name: Display name for the client
        ad_account_id: Meta ad account ID
        transaction: Single transaction dict from get_transactions()
        campaigns: Campaign-level spend for this transaction's period
        adsets: Ad-set level spend for this transaction's period
        base_dir: Output folder

    Returns:
        Path to the generated PDF, or None on failure
    """
    safe_account = ad_account_id.replace("act_", "")
    root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
    dest = root / safe_account
    dest.mkdir(parents=True, exist_ok=True)

    txn_id = transaction.get("id", "unknown")
    amount = float(transaction.get("amount", 0))
    txn_time = transaction.get("time", "")
    currency = transaction.get("currency", "USD")

    # Parse transaction time
    try:
        dt = datetime.fromisoformat(txn_time.replace("Z", "+00:00").replace("+0000", "+00:00"))
        date_str = dt.strftime("%b %d, %Y, %I:%M %p")
        date_short = dt.strftime("%Y%m%d_%H%M")
    except Exception:
        date_str = txn_time[:19] if txn_time else "Unknown"
        date_short = "unknown"

    filepath = dest / f"receipt_{safe_account}_{date_short}_{txn_id.replace('-', '_')[:20]}.pdf"

    try:
        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=letter,
            leftMargin=0.75 * inch,
            rightMargin=0.75 * inch,
            topMargin=0.6 * inch,
            bottomMargin=0.6 * inch,
        )

        story = []
        W = 7 * inch  # usable width

        # ── Header ─────────────────────────────────────────────────────────
        header_data = [[
            [Paragraph(f"Receipt for {client_name}", S_TITLE),
             Paragraph(f"Account ID: {safe_account}", S_SUBTITLE)],
            Paragraph("&#8734; Meta", S_META_LOGO),
        ]]
        ht = Table(header_data, colWidths=[5 * inch, 2 * inch])
        ht.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(ht)
        story.append(Spacer(1, 0.05 * inch))
        story.append(HRFlowable(width="100%", thickness=1.5, color=META_BLUE))
        story.append(Spacer(1, 0.2 * inch))

        # ── Invoice/Payment Date ───────────────────────────────────────────
        story.append(Paragraph("Invoice/Payment Date", S_LABEL))
        story.append(Paragraph(date_str, S_VALUE))

        # ── Transaction ID + Paid/Amount (two-column) ─────────────────────
        story.append(Spacer(1, 0.1 * inch))
        txn_data = [[
            [Paragraph("Transaction ID", S_LABEL),
             Paragraph(txn_id, S_VALUE)],
            [Paragraph("Paid", S_PAID),
             Paragraph(f"${amount:,.2f}", S_AMOUNT)],
        ]]
        txn_table = Table(txn_data, colWidths=[4.5 * inch, 2.5 * inch])
        txn_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(txn_table)

        # ── Product Type ───────────────────────────────────────────────────
        story.append(Paragraph("Product Type", S_LABEL))
        story.append(Paragraph("Meta ads", S_VALUE))

        story.append(Spacer(1, 0.15 * inch))
        story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))

        # ── Campaigns ──────────────────────────────────────────────────────
        story.append(Paragraph("Campaigns", S_SECTION))

        if campaigns:
            # Group adsets by campaign
            adsets_by_campaign = {}
            for a in adsets:
                cid = a.get("campaign_id", "")
                adsets_by_campaign.setdefault(cid, []).append(a)

            for c in sorted(campaigns, key=lambda x: -float(x.get("spend", 0))):
                camp_name = c.get("campaign_name", "Campaign")
                camp_spend = float(c.get("spend", 0))
                camp_start = c.get("date_start", "")
                camp_end = c.get("date_stop", "")
                camp_id = c.get("campaign_id", "")

                # Campaign name + spend
                row = [[
                    Paragraph(f"<b>{camp_name}</b>", S_CAMP_NAME),
                    Paragraph(f"${camp_spend:,.2f}", _s("R", fontSize=10, fontName="Helvetica-Bold", alignment=2)),
                ]]
                ct = Table(row, colWidths=[5 * inch, 2 * inch])
                ct.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
                story.append(ct)

                # Date range
                if camp_start and camp_end:
                    story.append(Paragraph(f"From {camp_start} to {camp_end}", S_CAMP_DATE))

                # Dashed separator
                story.append(Spacer(1, 0.05 * inch))
                story.append(HRFlowable(width="100%", thickness=0.5, color=ORANGE_DASH, dashArray=[3, 3]))
                story.append(Spacer(1, 0.05 * inch))

                # Ad sets under this campaign
                camp_adsets = adsets_by_campaign.get(camp_id, [])
                if camp_adsets:
                    for a in camp_adsets:
                        adset_name = a.get("adset_name", "Ad Set")
                        adset_impr = int(a.get("impressions", 0) or 0)
                        adset_spend = float(a.get("spend", 0))
                        arow = [[
                            Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{adset_name}", S_ADSET),
                            Paragraph(f"{adset_impr:,} Impressions", S_ADSET),
                            Paragraph(f"${adset_spend:,.2f}", _s("AR", fontSize=8, textColor=TEXT_SECONDARY, alignment=2)),
                        ]]
                        at = Table(arow, colWidths=[3.2 * inch, 2 * inch, 1.8 * inch])
                        at.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
                        story.append(at)
                else:
                    # No adset detail — show campaign impressions
                    camp_impr = int(c.get("impressions", 0) or 0)
                    if camp_impr:
                        story.append(Paragraph(
                            f"&nbsp;&nbsp;&nbsp;&nbsp;{camp_impr:,} Impressions &nbsp;&nbsp; ${camp_spend:,.2f}",
                            S_ADSET))

                story.append(Spacer(1, 0.1 * inch))
        else:
            story.append(Paragraph(f"Total ad spend: ${amount:,.2f}", S_VALUE))

        # ── Footer ─────────────────────────────────────────────────────────
        story.append(Spacer(1, 0.3 * inch))
        footer_data = [[
            [Paragraph("Meta Platforms, Inc.", S_FOOTER),
             Paragraph("1 Meta Way", S_FOOTER),
             Paragraph("Menlo Park, CA 94025", S_FOOTER),
             Paragraph("United States", S_FOOTER)],
            [Paragraph(f"<b>{client_name}</b>", S_FOOTER),
             Paragraph("Managed by Politika NYC", S_FOOTER),
             Paragraph("info@politikanyc.com", S_FOOTER),
             Paragraph("", S_FOOTER)],
        ]]
        ft = Table(footer_data, colWidths=[3.5 * inch, 3.5 * inch])
        ft.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        story.append(ft)

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
    """Legacy wrapper — generates a single summary PDF if no transactions are available."""
    # This is kept for backward compatibility but the orchestrator
    # now calls generate_transaction_pdf() per billing event instead.
    if not receipts:
        return None

    safe_account = ad_account_id.replace("act_", "")
    root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
    dest = root / safe_account
    dest.mkdir(parents=True, exist_ok=True)

    period_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    filepath = dest / f"receipt_{safe_account}_{period_str}.pdf"

    total_spend = sum(float(r.get("amount", 0)) for r in receipts)

    # Build a fake transaction for the legacy path
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
