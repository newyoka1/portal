"""
PDF receipt generator — pixel-accurate replica of Meta's TCPDF billing receipt.

Layout (A4 page, matches real Meta receipts):

  Receipt for [Client Name]                              (title, ~14pt, dark blue)
  Account ID: XXXXXXXXX                                  (subtitle, ~9pt, gray)
  ─────────────────────────── blue line ──────────────────

  Invoice/Payment Date                                   (label, ~8pt, blue)
  Mar 13, 2026, 3:52 PM                                  (value, ~10pt, black)

  Payment method                                         (label)
  [card info]                                            (value)
                                                          Paid         (right, gray ~12pt)
  Transaction ID                                         (label)
  XXXXXXX-XXXXXXX                                        $42.57       (right, black ~22pt bold)

  Product Type                                           (label)      You requested this manual payment.
  Meta ads                                               (value)

  ─────────────────────── thin gray line ─────────────────

  Campaigns                                              (section heading, ~12pt bold)

  Campaign Name                                          $XX.XX
  From [date] to [date]
  · · · · · · · · · · · · · orange dashed line · · · · ·
      AdSet Name                 X,XXX Impressions       $XX.XX

  ─────────────────────── thin gray line ─────────────────

  Meta Platforms, Inc.              [Client Name]
  1 Meta Way                       [Client Address]
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
from reportlab.lib.units import inch, mm
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    HRFlowable,
)

from src.config import RECEIPT_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

# ── Colors (sampled from real Meta receipt) ──────────────────────────────────
C_TITLE      = colors.HexColor("#1C2B33")   # dark blue-black for title
C_BLUE_LINK  = colors.HexColor("#0E71EB")   # blue for labels
C_BLUE_LINE  = colors.HexColor("#0E71EB")   # header separator
C_BLACK      = colors.HexColor("#1C1E21")   # body text
C_GRAY       = colors.HexColor("#65676B")   # secondary text
C_LIGHT_GRAY = colors.HexColor("#DADDE1")   # thin separator lines
C_ORANGE     = colors.HexColor("#E8913A")   # dashed campaign separator
C_META_BLUE  = colors.HexColor("#0668E1")   # Meta logo color
C_FOOTER     = colors.HexColor("#8A8D91")   # footer text

# ── Styles ───────────────────────────────────────────────────────────────────
def _s(name, **kw):
    d = {"fontName": "Helvetica", "textColor": C_BLACK, "fontSize": 10, "leading": 13}
    d.update(kw)
    return ParagraphStyle(name, **d)

S_TITLE      = _s("T", fontSize=14, fontName="Helvetica-Bold", textColor=C_TITLE, leading=18)
S_ACCT_ID    = _s("A", fontSize=9, textColor=C_GRAY, leading=12)
S_LABEL      = _s("L", fontSize=8, textColor=C_BLUE_LINK, leading=11, spaceBefore=8)
S_VALUE      = _s("V", fontSize=10, textColor=C_BLACK, leading=13, spaceAfter=2)
S_PAID_LABEL = _s("PL", fontSize=12, textColor=C_GRAY, alignment=2, leading=15)
S_PAID_AMT   = _s("PA", fontSize=22, fontName="Helvetica-Bold", textColor=C_BLACK, alignment=2, leading=26)
S_PAID_NOTE  = _s("PN", fontSize=8, textColor=C_GRAY, alignment=2, leading=11)
S_SECTION    = _s("S", fontSize=12, fontName="Helvetica-Bold", textColor=C_BLACK, spaceBefore=10, spaceAfter=6, leading=16)
S_CAMP       = _s("C", fontSize=10, fontName="Helvetica-Bold", textColor=C_BLACK, leading=13)
S_CAMP_SPEND = _s("CS", fontSize=10, fontName="Helvetica", textColor=C_BLACK, alignment=2, leading=13)
S_CAMP_DATE  = _s("CD", fontSize=8, textColor=C_GRAY, leading=11)
S_ADSET_NAME = _s("AN", fontSize=8, textColor=C_GRAY, leading=11)
S_ADSET_IMPR = _s("AI", fontSize=8, textColor=C_GRAY, alignment=1, leading=11)
S_ADSET_SPEND= _s("AS", fontSize=8, textColor=C_BLACK, alignment=2, leading=11)
S_FOOTER     = _s("F", fontSize=7, textColor=C_FOOTER, leading=10)
S_TCPDF      = _s("TC", fontSize=6, textColor=C_LIGHT_GRAY, alignment=1, leading=8)


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
    currency = transaction.get("currency", "USD")

    try:
        dt = datetime.fromisoformat(txn_time.replace("Z", "+00:00").replace("+0000", "+00:00"))
        date_str = dt.strftime("%b %d, %Y, %I:%M %p").replace(" 0", " ")
        date_file = dt.strftime("%Y%m%d_%H%M")
    except Exception:
        date_str = txn_time[:19] if txn_time else "Unknown"
        date_file = "unknown"

    filepath = dest / f"receipt_{safe_account}_{date_file}.pdf"

    try:
        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=A4,
            leftMargin=15 * mm,
            rightMargin=15 * mm,
            topMargin=12 * mm,
            bottomMargin=12 * mm,
        )
        W = A4[0] - 30 * mm  # usable width
        story = []

        # ── Title + Account ID ─────────────────────────────────────────────
        story.append(Paragraph(f"Receipt for {client_name}", S_TITLE))
        story.append(Paragraph(f"Account ID: {safe_account}", S_ACCT_ID))
        story.append(Spacer(1, 2 * mm))
        story.append(HRFlowable(width="100%", thickness=1.5, color=C_BLUE_LINE))
        story.append(Spacer(1, 5 * mm))

        # ── Invoice/Payment Date ───────────────────────────────────────────
        story.append(Paragraph("Invoice/Payment Date", S_LABEL))
        story.append(Paragraph(date_str, S_VALUE))

        # ── Transaction ID + Paid amount (side by side) ────────────────────
        story.append(Spacer(1, 2 * mm))

        left_col = [
            Paragraph("Transaction ID", S_LABEL),
            Paragraph(txn_id, S_VALUE),
            Spacer(1, 2 * mm),
            Paragraph("Product Type", S_LABEL),
            Paragraph("Meta ads", S_VALUE),
        ]
        right_col = [
            Paragraph("Paid", S_PAID_LABEL),
            Paragraph(f"${amount:,.2f} {currency}", S_PAID_AMT),
            Spacer(1, 1 * mm),
            Paragraph("", S_PAID_NOTE),
        ]

        info_data = [[left_col, right_col]]
        info_table = Table(info_data, colWidths=[W * 0.55, W * 0.45])
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

        # ── Campaigns ──────────────────────────────────────────────────────
        story.append(Paragraph("Campaigns", S_SECTION))

        # Group adsets by campaign_id
        adsets_by_campaign = {}
        for a in adsets:
            cid = a.get("campaign_id", "")
            adsets_by_campaign.setdefault(cid, []).append(a)

        if campaigns:
            for c in sorted(campaigns, key=lambda x: -float(x.get("spend", 0))):
                camp_name = c.get("campaign_name", "Campaign")
                camp_spend = float(c.get("spend", 0))
                camp_id = c.get("campaign_id", "")
                camp_start = c.get("date_start", "")
                camp_end = c.get("date_stop", "")

                # Format dates like Meta: "From Oct 29, 2025, 12:00 AM to Oct 31, 2025, 5:33 AM"
                try:
                    ds = datetime.strptime(camp_start, "%Y-%m-%d").strftime("%b %d, %Y")
                    de = datetime.strptime(camp_end, "%Y-%m-%d").strftime("%b %d, %Y")
                    date_range = f"From {ds}, 12:00 AM to {de}, 11:59 PM"
                except Exception:
                    date_range = f"From {camp_start} to {camp_end}" if camp_start else ""

                # Campaign name + spend on same line
                camp_row = [[
                    Paragraph(f"<b>{camp_name}</b>", S_CAMP),
                    Paragraph(f"${camp_spend:,.2f}", S_CAMP_SPEND),
                ]]
                ct = Table(camp_row, colWidths=[W * 0.75, W * 0.25])
                ct.setStyle(TableStyle([
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]))
                story.append(ct)

                # Date range
                if date_range:
                    story.append(Paragraph(date_range, S_CAMP_DATE))

                # Orange dashed separator (matches Meta's dotted line between campaign header and adsets)
                story.append(Spacer(1, 1.5 * mm))
                story.append(HRFlowable(width="100%", thickness=0.5, color=C_ORANGE, dash=[2, 2]))
                story.append(Spacer(1, 1.5 * mm))

                # Ad sets under this campaign
                camp_adsets = adsets_by_campaign.get(camp_id, [])
                if camp_adsets:
                    for a in camp_adsets:
                        adset_name = a.get("adset_name", "Ad Set")
                        # Truncate long names with ... (Meta truncates at ~60 chars)
                        if len(adset_name) > 60:
                            adset_name = adset_name[:57] + "..."
                        adset_impr = int(a.get("impressions", 0) or 0)
                        adset_spend = float(a.get("spend", 0))

                        adset_row = [[
                            Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{adset_name}", S_ADSET_NAME),
                            Paragraph(f"{adset_impr:,} Impressions", S_ADSET_IMPR),
                            Paragraph(f"${adset_spend:,.2f}", S_ADSET_SPEND),
                        ]]
                        at = Table(adset_row, colWidths=[W * 0.50, W * 0.28, W * 0.22])
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
                        impr_row = [[
                            Paragraph("", S_ADSET_NAME),
                            Paragraph(f"{camp_impr:,} Impressions", S_ADSET_IMPR),
                            Paragraph(f"${camp_spend:,.2f}", S_ADSET_SPEND),
                        ]]
                        it = Table(impr_row, colWidths=[W * 0.50, W * 0.28, W * 0.22])
                        it.setStyle(TableStyle([
                            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 0),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                        ]))
                        story.append(it)

                story.append(Spacer(1, 3 * mm))
        else:
            # No campaign data available
            story.append(Paragraph(f"Total ad spend: ${amount:,.2f} {currency}", S_VALUE))

        # ── Footer ─────────────────────────────────────────────────────────
        story.append(Spacer(1, 8 * mm))

        meta_addr = [
            Paragraph("Meta Platforms, Inc.", S_FOOTER),
            Paragraph("1 Meta Way", S_FOOTER),
            Paragraph("Menlo Park, CA 94025", S_FOOTER),
            Paragraph("United States", S_FOOTER),
        ]
        client_addr = [
            Paragraph(f"<b>{client_name}</b>", S_FOOTER),
        ]
        if client_address:
            for line in client_address.split("\n"):
                client_addr.append(Paragraph(line.strip(), S_FOOTER))
        else:
            client_addr.append(Paragraph("", S_FOOTER))

        # Pad to same length
        while len(client_addr) < len(meta_addr):
            client_addr.append(Paragraph("", S_FOOTER))
        while len(meta_addr) < len(client_addr):
            meta_addr.append(Paragraph("", S_FOOTER))

        footer_data = [[meta_addr[i], client_addr[i]] for i in range(len(meta_addr))]
        ft = Table(footer_data, colWidths=[W * 0.5, W * 0.5])
        ft.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(ft)

        # TCPDF footer (Meta uses this)
        story.append(Spacer(1, 3 * mm))
        story.append(Paragraph("Powered by TCPDF (www.tcpdf.org)", S_TCPDF))

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
    Generate a Meta-style receipt PDF from parsed Gmail receipt email data.

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
    product_type = receipt.get("product_type", "Meta ads")
    date_range_start = receipt.get("date_range_start", "")
    date_range_end = receipt.get("date_range_end", "")
    email_date = receipt.get("email_date", "")
    currency = receipt.get("currency", "USD")

    # Invoice date from email
    try:
        dt = datetime.fromisoformat(email_date)
        date_str = dt.strftime("%b %d, %Y, %I:%M %p").replace(" 0", " ")
        date_file = dt.strftime("%Y%m%d_%H%M")
    except Exception:
        date_str = email_date[:19] if email_date else "Unknown"
        date_file = "unknown"

    filepath = dest / f"receipt_{acct_id}_{date_file}.pdf"

    try:
        doc = SimpleDocTemplate(
            str(filepath), pagesize=A4,
            leftMargin=15*mm, rightMargin=15*mm,
            topMargin=12*mm, bottomMargin=12*mm,
        )
        W = A4[0] - 30*mm
        story = []

        # ── Title ──────────────────────────────────────────────────────────
        story.append(Paragraph(f"Receipt for {client_name}", S_TITLE))
        story.append(Paragraph(f"Account ID: {acct_id}", S_ACCT_ID))
        story.append(Spacer(1, 2*mm))
        story.append(HRFlowable(width="100%", thickness=1.5, color=C_BLUE_LINE))
        story.append(Spacer(1, 5*mm))

        # ── Invoice/Payment Date ───────────────────────────────────────────
        story.append(Paragraph("Invoice/Payment Date", S_LABEL))
        story.append(Paragraph(date_str, S_VALUE))

        # ── Payment method ─────────────────────────────────────────────────
        if payment_method:
            story.append(Paragraph("Payment method", S_LABEL))
            story.append(Paragraph(payment_method, S_VALUE))
            if reference_number:
                story.append(Paragraph(f"Reference Number: {reference_number}", S_VALUE))

        # ── Transaction ID + Paid (side by side) ──────────────────────────
        story.append(Spacer(1, 2*mm))
        left = [
            Paragraph("Transaction ID", S_LABEL),
            Paragraph(txn_id, S_VALUE),
        ]
        right = [
            Paragraph("Paid", S_PAID_LABEL),
            Paragraph(f"${amount:,.2f} {currency}", S_PAID_AMT),
        ]
        if billing_reason:
            right.append(Paragraph(billing_reason, S_PAID_NOTE))

        info = Table([[left, right]], colWidths=[W*0.55, W*0.45])
        info.setStyle(TableStyle([
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("LEFTPADDING", (0,0), (-1,-1), 0),
            ("RIGHTPADDING", (0,0), (-1,-1), 0),
            ("TOPPADDING", (0,0), (-1,-1), 0),
            ("BOTTOMPADDING", (0,0), (-1,-1), 0),
        ]))
        story.append(info)

        # ── Product Type ───────────────────────────────────────────────────
        story.append(Paragraph("Product Type", S_LABEL))
        story.append(Paragraph(product_type, S_VALUE))

        story.append(Spacer(1, 5*mm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=C_LIGHT_GRAY))

        # ── Campaigns ──────────────────────────────────────────────────────
        story.append(Paragraph("Campaigns", S_SECTION))

        adsets_by_campaign = {}
        for a in adsets:
            cid = a.get("campaign_id", "")
            adsets_by_campaign.setdefault(cid, []).append(a)

        if campaigns:
            for c in sorted(campaigns, key=lambda x: -float(x.get("spend", 0))):
                camp_name = c.get("campaign_name", "Campaign")
                camp_spend = float(c.get("spend", 0))
                camp_id = c.get("campaign_id", "")
                cs = c.get("date_start", "")
                ce = c.get("date_stop", "")

                try:
                    ds = datetime.strptime(cs, "%Y-%m-%d").strftime("%b %d, %Y")
                    de = datetime.strptime(ce, "%Y-%m-%d").strftime("%b %d, %Y")
                    dr = f"From {ds}, 12:00 AM to {de}, 11:59 PM"
                except Exception:
                    dr = f"From {cs} to {ce}" if cs else ""

                # Campaign row
                ct = Table([[
                    Paragraph(f"<b>{camp_name}</b>", S_CAMP),
                    Paragraph(f"${camp_spend:,.2f}", S_CAMP_SPEND),
                ]], colWidths=[W*0.75, W*0.25])
                ct.setStyle(TableStyle([
                    ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
                    ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
                    ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
                ]))
                story.append(ct)
                if dr:
                    story.append(Paragraph(dr, S_CAMP_DATE))

                # Dashed separator
                story.append(Spacer(1, 1.5*mm))
                story.append(HRFlowable(width="100%", thickness=0.5, color=C_ORANGE, dash=[2,2]))
                story.append(Spacer(1, 1.5*mm))

                # Ad sets
                for a in adsets_by_campaign.get(camp_id, []):
                    aname = a.get("adset_name", "Ad Set")
                    if len(aname) > 60:
                        aname = aname[:57] + "..."
                    aimpr = int(a.get("impressions", 0) or 0)
                    aspend = float(a.get("spend", 0))
                    at = Table([[
                        Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{aname}", S_ADSET_NAME),
                        Paragraph(f"{aimpr:,} Impressions", S_ADSET_IMPR),
                        Paragraph(f"${aspend:,.2f}", S_ADSET_SPEND),
                    ]], colWidths=[W*0.50, W*0.28, W*0.22])
                    at.setStyle(TableStyle([
                        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
                        ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
                        ("TOPPADDING",(0,0),(-1,-1),1),("BOTTOMPADDING",(0,0),(-1,-1),1),
                    ]))
                    story.append(at)

                story.append(Spacer(1, 3*mm))
        else:
            story.append(Paragraph(f"Total ad spend: ${amount:,.2f} {currency}", S_VALUE))

        # Ad images are attached to the email separately, not embedded in the PDF

        # ── Footer ─────────────────────────────────────────────────────────
        story.append(Spacer(1, 8*mm))
        ft = Table([
            [Paragraph("Meta Platforms, Inc.", S_FOOTER), Paragraph(f"<b>{client_name}</b>", S_FOOTER)],
            [Paragraph("1 Meta Way", S_FOOTER), Paragraph("Managed by Politika NYC", S_FOOTER)],
            [Paragraph("Menlo Park, CA 94025", S_FOOTER), Paragraph("info@politikanyc.com", S_FOOTER)],
            [Paragraph("United States", S_FOOTER), Paragraph("", S_FOOTER)],
        ], colWidths=[W*0.5, W*0.5])
        ft.setStyle(TableStyle([
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
            ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
        ]))
        story.append(ft)
        story.append(Spacer(1, 3*mm))
        story.append(Paragraph("Powered by TCPDF (www.tcpdf.org)", S_TCPDF))

        doc.build(story)
        logger.info("Generated email receipt PDF: %s", filepath)
        return filepath

    except Exception as e:
        logger.error("Failed to generate email receipt PDF: %s", e)
        return None
