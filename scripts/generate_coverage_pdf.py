#!/usr/bin/env python3
from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path

from reportlab.lib import colors  # type: ignore[import-untyped]
from reportlab.lib.pagesizes import A4  # type: ignore[import-untyped]
from reportlab.lib.units import mm  # type: ignore[import-untyped]
from reportlab.pdfbase.pdfmetrics import stringWidth  # type: ignore[import-untyped]
from reportlab.pdfgen import canvas  # type: ignore[import-untyped]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a simple PDF summary from coverage.xml.")
    parser.add_argument("--xml", required=True, help="Path to coverage XML report")
    parser.add_argument("--output", required=True, help="Path to output PDF file")
    parser.add_argument("--title", required=True, help="Report title")
    parser.add_argument("--top-files", type=int, default=25, help="Number of files to include")
    return parser.parse_args()


def truncate(text: str, max_width: float, font_name: str, font_size: int) -> str:
    if stringWidth(text, font_name, font_size) <= max_width:
        return text
    ellipsis = "..."
    while text and stringWidth(text + ellipsis, font_name, font_size) > max_width:
        text = text[:-1]
    return text + ellipsis


def draw_header(pdf: canvas.Canvas, title: str, page_width: float, page_height: float) -> float:
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(20 * mm, page_height - 20 * mm, title)
    pdf.setStrokeColor(colors.lightgrey)
    pdf.line(20 * mm, page_height - 23 * mm, page_width - 20 * mm, page_height - 23 * mm)
    return page_height - 32 * mm


def ensure_space(pdf: canvas.Canvas, y: float, required: float, title: str, page_width: float, page_height: float) -> float:
    if y >= required:
        return y
    pdf.showPage()
    return draw_header(pdf, title, page_width, page_height)


def main() -> None:
    args = parse_args()
    root = ET.parse(args.xml).getroot()

    total_line_rate = float(root.attrib.get("line-rate", 0.0)) * 100
    lines_valid = int(root.attrib.get("lines-valid", 0))
    lines_covered = int(root.attrib.get("lines-covered", 0))

    files: list[tuple[str, float, int, int]] = []
    for class_node in root.findall(".//class"):
        filename = class_node.attrib.get("filename", "unknown")
        rate = float(class_node.attrib.get("line-rate", 0.0)) * 100
        line_nodes = class_node.findall("./lines/line")
        valid = len(line_nodes)
        covered = sum(1 for line in line_nodes if int(line.attrib.get("hits", "0")) > 0)
        files.append((filename, rate, covered, valid))

    files.sort(key=lambda item: (item[1], item[0]))
    files = files[: args.top_files]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    page_width, page_height = A4
    pdf = canvas.Canvas(str(output_path), pagesize=A4)
    y = draw_header(pdf, args.title, page_width, page_height)

    pdf.setFont("Helvetica", 11)
    summary_lines = [
        f"Overall line coverage: {total_line_rate:.2f}%",
        f"Covered lines: {lines_covered}/{lines_valid}",
        f"Files listed below: {len(files)} lowest-coverage files",
    ]
    for line in summary_lines:
        pdf.drawString(20 * mm, y, line)
        y -= 7 * mm

    y -= 2 * mm
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(20 * mm, y, "File")
    pdf.drawString(135 * mm, y, "Coverage")
    pdf.drawString(165 * mm, y, "Covered/Total")
    y -= 5 * mm
    pdf.setStrokeColor(colors.lightgrey)
    pdf.line(20 * mm, y, page_width - 20 * mm, y)
    y -= 6 * mm

    for filename, rate, covered, valid in files:
        y = ensure_space(pdf, y, 25 * mm, args.title, page_width, page_height)
        pdf.setFont("Helvetica", 9)
        pdf.drawString(20 * mm, y, truncate(filename, 110 * mm, "Helvetica", 9))
        pdf.drawRightString(160 * mm, y, f"{rate:.2f}%")
        pdf.drawRightString(page_width - 20 * mm, y, f"{covered}/{valid}")
        y -= 5 * mm

    pdf.save()


if __name__ == "__main__":
    main()
