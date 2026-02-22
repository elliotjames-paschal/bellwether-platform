#!/usr/bin/env python3
"""
Combine DOF figures into single PDF with section outline headers only.
"""

import os
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# Paths
BASE_DIR = Path(__file__).parent.parent
FIGURES_DIR = BASE_DIR / "output" / "dof_figures_v3"
OUTPUT_PDF = BASE_DIR / "output" / "dof_figures_combined.pdf"

# Document structure - section headers only, no explanations
SECTIONS = [
    {
        "header": "1. Calibration",
        "figures": ["fig01_calibration.png"]
    },
    {
        "header": "2. Score Distributions",
        "figures": ["fig02_brier_kde.png"]
    },
    {
        "header": "3. Platform Ranking Shifts",
        "figures": ["fig03_platform_ranking_shifts.png"]
    },
    {
        "header": "4. Spot vs VWAP",
        "figures": ["fig04_spot_vs_vwap.png"]
    },
    {
        "header": "5. Liquidity",
        "figures": ["fig05_volume_cdf.png"]
    },
    {
        "header": "6. Volume Thresholds",
        "figures": ["fig06_brier_by_threshold.png"]
    },
    {
        "header": "7. Category Heterogeneity",
        "figures": ["fig07_accuracy_by_category.png"]
    },
    {
        "header": "8. Platform Comparison",
        "figures": ["fig08_platform_comparison.png"]
    },
    {
        "header": "9. Specification Curve (Brier Score)",
        "figures": ["fig09a_specification_curve_brier.png"]
    },
    {
        "header": "10. Specification Curve (Log Loss)",
        "figures": ["fig09b_specification_curve_logloss.png"]
    },
]

def main():
    # Create styles
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        'Title',
        parent=styles['Heading1'],
        fontSize=18,
        alignment=TA_CENTER,
        spaceAfter=30,
        fontName='Times-Bold'
    )

    section_style = ParagraphStyle(
        'Section',
        parent=styles['Heading2'],
        fontSize=14,
        alignment=TA_LEFT,
        spaceBefore=20,
        spaceAfter=12,
        fontName='Times-Bold'
    )

    # Build document
    doc = SimpleDocTemplate(
        str(OUTPUT_PDF),
        pagesize=letter,
        leftMargin=0.75*inch,
        rightMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )

    story = []

    # Title
    story.append(Paragraph("Researcher Degrees of Freedom in Prediction Market Evaluation", title_style))
    story.append(Spacer(1, 0.3*inch))

    # Data summary (brief)
    data_style = ParagraphStyle(
        'Data',
        parent=styles['Normal'],
        fontSize=10,
        alignment=TA_LEFT,
        fontName='Times-Roman'
    )
    story.append(Paragraph("<b>Data:</b> 22,837 political markets (14,856 PM with 8.8M trades, 7,981 Kalshi with 796K trades)", data_style))
    story.append(Paragraph("<b>Truncation:</b> PM: 48h/24h/12h before anchor; Kalshi: 24h/12h/3h before anchor", data_style))
    story.append(Paragraph("<b>Specifications:</b> 4 truncation × 6 price × 4 threshold × 2 metric = 192", data_style))
    story.append(Spacer(1, 0.3*inch))

    # Sections with figures
    for section in SECTIONS:
        story.append(Paragraph(section["header"], section_style))

        for fig_name in section["figures"]:
            fig_path = FIGURES_DIR / fig_name
            if fig_path.exists():
                # Calculate image size to fit page width
                img = Image(str(fig_path), width=6.5*inch, height=4.5*inch)
                img.hAlign = 'CENTER'
                story.append(img)
                story.append(Spacer(1, 0.2*inch))
            else:
                story.append(Paragraph(f"[Figure not found: {fig_name}]", styles['Normal']))

        story.append(Spacer(1, 0.1*inch))

    # Build PDF
    doc.build(story)
    print(f"Combined PDF saved to: {OUTPUT_PDF}")

if __name__ == "__main__":
    main()
