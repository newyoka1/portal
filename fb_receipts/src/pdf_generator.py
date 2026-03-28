"""
PDF receipt generator — emulates Facebook/Meta invoice style using reportlab.

Matches the real Meta Ads receipt layout:
- Header: "Receipt for [Client Name]"
- Account ID, Invoice Date, Payment method, Transaction ID
- Product Type: Meta ads / Paid amount
- Campaign breakdown with ad set detail
- Meta Platforms address + client address footer
"""

import logging
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    HRFlowable, Image as RLImage,
)

from src.config import RECEIPT_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

# Colors matching Meta's invoice style
META_BLUE     = colors.HexColor("#1877F2")
TEXT_PRIMARY   = colors.HexColor("#1C1E21")
TEXT_SECONDARY = colors.HexColor("#65676B")
TEXT_LIGHT     = colors.HexColor("#8A8D91")
BG_LIGHT       = colors.HexColor("#F7F8FA")
BORDER         = colors.HexColor("#DADDE1")


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
    """
    Generate a Meta-style invoice PDF.

    Returns Path to the generated PDF, or None on failure.
    """
    if not receipts:
        return None

    safe_account = ad_account_id.replace("act_", "")
    root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
    dest = root / safe_account
    dest.mkdir(parents=True, exist_ok=True)

    period_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    filepath = dest / f"receipt_{safe_account}_{period_str}.pdf"

    try:
        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=letter,
            leftMargin=0.75 * inch,
            rightMargin=0.75 * inch,
            topMargin=0.6 * inch,
            bottomMargin=0.6 * inch,
        )

        styles = getSampleStyleSheet()
        story = []

        # ── Styles ─────────────────────────────────────────────────────────
        s_title = ParagraphStyle("Title", parent=styles["Normal"],
            fontSize=16, textColor=TEXT_PRIMARY, fontName="Helvetica-Bold",
            spaceAfter=12)
        s_label = ParagraphStyle("Label", parent=styles["Normal"],
            fontSize=9, textColor=TEXT_SECONDARY, spaceAfter=1)
        s_value = ParagraphStyle("Value", parent=styles["Normal"],
            fontSize=10, textColor=TEXT_PRIMARY, spaceAfter=8)
        s_value_bold = ParagraphStyle("ValueBold", parent=styles["Normal"],
            fontSize=10, textColor=TEXT_PRIMARY, fontName="Helvetica-Bold",
            spaceAfter=8)
        s_section = ParagraphStyle("Section", parent=styles["Normal"],
            fontSize=11, textColor=TEXT_PRIMARY, fontName="Helvetica-Bold",
            spaceBefore=14, spaceAfter=6)
        s_amount_large = ParagraphStyle("AmountLarge", parent=styles["Normal"],
            fontSize=20, textColor=TEXT_PRIMARY, fontName="Helvetica-Bold",
            spaceAfter=2)
        s_product = ParagraphStyle("Product", parent=styles["Normal"],
            fontSize=10, textColor=TEXT_SECONDARY, spaceAfter=2)
        s_campaign_name = ParagraphStyle("CampName", parent=styles["Normal"],
            fontSize=10, textColor=TEXT_PRIMARY, fontName="Helvetica-Bold",
            spaceAfter=2)
        s_adset = ParagraphStyle("AdSet", parent=styles["Normal"],
            fontSize=8, textColor=TEXT_SECONDARY, spaceAfter=1)
        s_footer_addr = ParagraphStyle("FooterAddr", parent=styles["Normal"],
            fontSize=8, textColor=TEXT_LIGHT, spaceAfter=1)
        s_footer_note = ParagraphStyle("FooterNote", parent=styles["Normal"],
            fontSize=7, textColor=TEXT_LIGHT, spaceAfter=1)

        # ── Compute totals ─────────────────────────────────────────────────
        total_spend = sum(float(r.get("amount", 0)) for r in receipts)
        total_impressions = sum(int(r.get("impressions", 0) or 0) for r in receipts)
        total_clicks = sum(int(r.get("clicks", 0) or 0) for r in receipts)

        # Transaction ID from period
        txn_id = f"{safe_account}-{start_date.strftime('%Y%m%d')}{end_date.strftime('%Y%m%d')}"

        # ── Header: "Receipt for [Client]" ─────────────────────────────────
        story.append(Paragraph(f"Receipt for {client_name}", s_title))
        story.append(Spacer(1, 0.05 * inch))

        # ── Info grid (two columns) ────────────────────────────────────────
        info_data = [
            [Paragraph("Account ID", s_label), Paragraph("Invoice/Payment Date", s_label)],
            [Paragraph(safe_account, s_value), Paragraph(
                f"{end_date.strftime('%b %d, %Y')}", s_value)],
            [Paragraph("Reference Number", s_label), Paragraph("Transaction ID", s_label)],
            [Paragraph(f"POL-{safe_account[-6:]}", s_value), Paragraph(txn_id, s_value)],
        ]
        info_table = Table(info_data, colWidths=[3.5 * inch, 3.5 * inch])
        info_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(info_table)
        story.append(Spacer(1, 0.1 * inch))
        story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))
        story.append(Spacer(1, 0.15 * inch))

        # ── Product Type + Amount ──────────────────────────────────────────
        story.append(Paragraph("Product Type", s_label))

        product_data = [[
            Paragraph("Meta ads", s_product),
            Paragraph("Paid", s_product),
        ], [
            Paragraph("", s_product),
            Paragraph(f"${total_spend:,.2f}", s_amount_large),
        ]]
        product_table = Table(product_data, colWidths=[5 * inch, 2 * inch])
        product_table.setStyle(TableStyle([
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        story.append(product_table)
        story.append(Spacer(1, 0.1 * inch))
        story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))
        story.append(Spacer(1, 0.15 * inch))

        # ── Campaigns ──────────────────────────────────────────────────────
        story.append(Paragraph("Campaigns", s_section))

        if campaigns:
            for c in sorted(campaigns, key=lambda x: -float(x.get("spend", 0))):
                camp_name = c.get("campaign_name", "Campaign")
                camp_spend = float(c.get("spend", 0))
                camp_impr = int(c.get("impressions", 0) or 0)
                camp_start = c.get("date_start", start_date.strftime("%Y-%m-%d"))
                camp_end = c.get("date_stop", end_date.strftime("%Y-%m-%d"))

                # Campaign header with spend aligned right
                camp_data = [[
                    Paragraph(camp_name, s_campaign_name),
                    Paragraph(f"${camp_spend:,.2f}", s_value_bold),
                ]]
                camp_table = Table(camp_data, colWidths=[5 * inch, 2 * inch])
                camp_table.setStyle(TableStyle([
                    ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ]))
                story.append(camp_table)

                # Period + impressions detail line
                story.append(Paragraph(
                    f"From {camp_start} to {camp_end}",
                    s_adset))
                if camp_impr:
                    story.append(Paragraph(
                        f"{camp_impr:,} Impressions &nbsp;&nbsp; ${camp_spend:,.2f}",
                        s_adset))
                story.append(Spacer(1, 0.08 * inch))
        else:
            # No campaign data — show daily summary as a single "campaign"
            story.append(Paragraph(
                f"Ad Spend: {start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')}",
                s_campaign_name))
            story.append(Paragraph(
                f"{total_impressions:,} Impressions &nbsp;&nbsp; {total_clicks:,} Clicks &nbsp;&nbsp; ${total_spend:,.2f}",
                s_adset))

        story.append(Spacer(1, 0.15 * inch))
        story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))
        story.append(Spacer(1, 0.15 * inch))

        # ── Ad Creatives (if available) ────────────────────────────────────
        if ad_images:
            valid_images = [p for p in ad_images if p.exists()]
            if valid_images:
                story.append(Paragraph("Ad Creatives", s_section))
                img_row = []
                for img_path in valid_images[:6]:
                    try:
                        img = RLImage(str(img_path), width=2.1 * inch, height=2.1 * inch,
                                      kind="proportional")
                        img_row.append(img)
                        if len(img_row) == 3:
                            it = Table([img_row], colWidths=[2.3*inch]*3)
                            it.setStyle(TableStyle([
                                ("ALIGN", (0,0), (-1,-1), "CENTER"),
                                ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                                ("TOPPADDING", (0,0), (-1,-1), 4),
                                ("BOTTOMPADDING", (0,0), (-1,-1), 4),
                            ]))
                            story.append(it)
                            img_row = []
                    except Exception:
                        pass
                if img_row:
                    img_row += [""] * (3 - len(img_row))
                    it = Table([img_row], colWidths=[2.3*inch]*3)
                    it.setStyle(TableStyle([("ALIGN",(0,0),(-1,-1),"CENTER")]))
                    story.append(it)
                story.append(Spacer(1, 0.15 * inch))
                story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER))
                story.append(Spacer(1, 0.15 * inch))

        # ── Performance metrics ────────────────────────────────────────────
        if total_impressions > 0:
            cpm = (total_spend / total_impressions) * 1000
            cpc = total_spend / total_clicks if total_clicks > 0 else 0
            ctr = (total_clicks / total_impressions) * 100

            perf_data = [
                ["Impressions", "Clicks", "CTR", "CPM", "CPC"],
                [f"{total_impressions:,}", f"{total_clicks:,}", f"{ctr:.2f}%",
                 f"${cpm:.2f}", f"${cpc:.2f}" if total_clicks else "—"],
            ]
            perf_table = Table(perf_data, colWidths=[1.4*inch]*5)
            perf_table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), BG_LIGHT),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 7),
                ("TEXTCOLOR", (0, 0), (-1, 0), TEXT_SECONDARY),
                ("FONTSIZE", (0, 1), (-1, 1), 10),
                ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("BOX", (0, 0), (-1, -1), 0.25, BORDER),
            ]))
            story.append(perf_table)
            story.append(Spacer(1, 0.2 * inch))

        # ── Footer: Meta address + Client address ──────────────────────────
        footer_left = [
            Paragraph("Meta Platforms, Inc.", s_footer_addr),
            Paragraph("1 Meta Way", s_footer_addr),
            Paragraph("Menlo Park, CA 94025", s_footer_addr),
            Paragraph("United States", s_footer_addr),
        ]
        footer_right = [
            Paragraph(f"<b>{client_name}</b>", s_footer_addr),
            Paragraph("Managed by Politika NYC", s_footer_addr),
            Paragraph("info@politikanyc.com", s_footer_addr),
            Paragraph("", s_footer_addr),
        ]

        footer_data = [[footer_left[i], footer_right[i]] for i in range(4)]
        footer_table = Table(footer_data, colWidths=[3.5 * inch, 3.5 * inch])
        footer_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(footer_table)

        story.append(Spacer(1, 0.15 * inch))
        story.append(Paragraph(
            "This receipt was generated from Meta Marketing API spend data. "
            "Amounts reflect actual ad spend as reported by Meta Ads Manager.",
            s_footer_note))

        doc.build(story)
        logger.info("Generated PDF receipt -> %s", filepath)
        return filepath

    except Exception as e:
        logger.error("Failed to generate PDF for %s: %s", ad_account_id, e)
        return None
