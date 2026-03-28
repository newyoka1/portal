"""
PDF receipt generator using reportlab.

Generates a single consolidated receipt PDF per ad account
covering the full billing period.
"""

import logging
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

from src.config import RECEIPT_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

BRAND_BLUE = colors.HexColor("#1877F2")  # Facebook blue
LIGHT_GRAY = colors.HexColor("#F5F5F5")
MID_GRAY = colors.HexColor("#DDDDDD")
DARK_GRAY = colors.HexColor("#333333")


def generate_receipt_pdf(
    client_name: str,
    ad_account_id: str,
    receipts: list[dict],
    start_date: datetime,
    end_date: datetime,
    base_dir: Path | None = None,
) -> Path | None:
    """
    Generate a PDF receipt for a given ad account and period.

    Args:
        client_name: Display name for the client
        ad_account_id: Meta ad account ID (with or without act_ prefix)
        receipts: List of daily spend dicts from MetaClient
        start_date: Start of billing period
        end_date: End of billing period
        base_dir: Root folder for this run (e.g. INVOICES/2026-03-10_2026-03-17/).
                  Defaults to RECEIPT_DOWNLOAD_DIR if not supplied.

    Returns:
        Path to the generated PDF, or None on failure
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
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
        )

        styles = getSampleStyleSheet()
        story = []

        # ── Header ──────────────────────────────────────────────────────────
        header_style = ParagraphStyle(
            "Header",
            parent=styles["Normal"],
            fontSize=22,
            textColor=BRAND_BLUE,
            spaceAfter=4,
            fontName="Helvetica-Bold",
        )
        sub_style = ParagraphStyle(
            "Sub",
            parent=styles["Normal"],
            fontSize=10,
            textColor=colors.gray,
            spaceAfter=2,
        )
        label_style = ParagraphStyle(
            "Label",
            parent=styles["Normal"],
            fontSize=10,
            textColor=DARK_GRAY,
            spaceAfter=2,
        )
        total_style = ParagraphStyle(
            "Total",
            parent=styles["Normal"],
            fontSize=13,
            textColor=DARK_GRAY,
            fontName="Helvetica-Bold",
            spaceAfter=4,
        )

        story.append(Paragraph("Facebook Advertising Receipt", header_style))
        story.append(Paragraph("Politika NYC · info@politikanyc.com", sub_style))
        story.append(Spacer(1, 0.15 * inch))

        # ── Client + period info ─────────────────────────────────────────────
        period = (
            f"{start_date.strftime('%B %d, %Y')} – {end_date.strftime('%B %d, %Y')}"
        )
        story.append(Paragraph(f"<b>Client:</b> {client_name}", label_style))
        story.append(Paragraph(f"<b>Ad Account:</b> act_{safe_account}", label_style))
        story.append(Paragraph(f"<b>Billing Period:</b> {period}", label_style))
        story.append(
            Paragraph(
                f"<b>Generated:</b> {datetime.now().strftime('%B %d, %Y')}",
                label_style,
            )
        )
        story.append(Spacer(1, 0.2 * inch))

        # ── Spend table ──────────────────────────────────────────────────────
        table_data = [["Date", "Spend (USD)", "Impressions", "Clicks"]]

        total_spend = 0.0
        total_impressions = 0
        total_clicks = 0

        for r in sorted(receipts, key=lambda x: x.get("date", "")):
            spend = float(r.get("amount", 0))
            impressions = int(r.get("impressions", 0) or 0)
            clicks = int(r.get("clicks", 0) or 0)
            total_spend += spend
            total_impressions += impressions
            total_clicks += clicks

            table_data.append([
                r.get("date", ""),
                f"${spend:,.2f}",
                f"{impressions:,}",
                f"{clicks:,}",
            ])

        # Totals row
        table_data.append([
            "TOTAL",
            f"${total_spend:,.2f}",
            f"{total_impressions:,}",
            f"{total_clicks:,}",
        ])

        col_widths = [1.8 * inch, 1.5 * inch, 1.8 * inch, 1.4 * inch]
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            # Header row
            ("BACKGROUND", (0, 0), (-1, 0), BRAND_BLUE),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            # Data rows — alternating background
            ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, LIGHT_GRAY]),
            ("FONTSIZE", (0, 1), (-1, -2), 9),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
            ("ALIGN", (0, 1), (0, -1), "LEFT"),
            ("TOPPADDING", (0, 1), (-1, -2), 5),
            ("BOTTOMPADDING", (0, 1), (-1, -2), 5),
            # Totals row
            ("BACKGROUND", (0, -1), (-1, -1), MID_GRAY),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, -1), (-1, -1), 10),
            ("TOPPADDING", (0, -1), (-1, -1), 7),
            ("BOTTOMPADDING", (0, -1), (-1, -1), 7),
            # Grid
            ("GRID", (0, 0), (-1, -1), 0.5, MID_GRAY),
            ("BOX", (0, 0), (-1, -1), 1, BRAND_BLUE),
        ]))

        story.append(table)
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph(f"Total Spend: ${total_spend:,.2f} USD", total_style))

        # ── Footer ───────────────────────────────────────────────────────────
        footer_style = ParagraphStyle(
            "Footer",
            parent=styles["Normal"],
            fontSize=8,
            textColor=colors.gray,
            spaceAfter=2,
        )
        story.append(Spacer(1, 0.3 * inch))
        story.append(Paragraph(
            "This receipt was generated automatically from Meta Ads Manager spend data. "
            "For questions contact info@politikanyc.com.",
            footer_style,
        ))

        doc.build(story)
        logger.info("Generated PDF receipt -> %s", filepath)
        return filepath

    except Exception as e:
        logger.error("Failed to generate PDF for %s: %s", ad_account_id, e)
        return None
