#!/usr/bin/env python3
from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

from reportlab.lib import colors  # type: ignore[import-untyped]
from reportlab.lib.pagesizes import A4  # type: ignore[import-untyped]
from reportlab.lib.units import mm  # type: ignore[import-untyped]
from reportlab.pdfbase.pdfmetrics import stringWidth  # type: ignore[import-untyped]
from reportlab.pdfgen import canvas  # type: ignore[import-untyped]


@dataclass
class SuiteSummary:
    name: str
    tests: int
    failures: int
    errors: int
    skipped: int
    time: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a PDF summary from one or more JUnit XML files.")
    parser.add_argument("--input", action="append", required=True, help="Path to JUnit XML file")
    parser.add_argument("--output", required=True, help="Path to output PDF file")
    parser.add_argument("--title", required=True, help="Report title")
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


def iter_suites(root: ET.Element) -> list[ET.Element]:
    if root.tag == "testsuite":
        return [root]
    if root.tag == "testsuites":
        return [node for node in root.findall("testsuite")]
    return root.findall(".//testsuite")


def main() -> None:
    args = parse_args()
    summaries: list[SuiteSummary] = []
    failed_cases: list[str] = []

    for file_name in args.input:
        root = ET.parse(file_name).getroot()
        for suite_node in iter_suites(root):
            name = suite_node.attrib.get("name", Path(file_name).stem)
            tests = int(suite_node.attrib.get("tests", 0))
            failures = int(suite_node.attrib.get("failures", 0))
            errors = int(suite_node.attrib.get("errors", 0))
            skipped = int(suite_node.attrib.get("skipped", 0))
            time = float(suite_node.attrib.get("time", 0.0))
            summaries.append(SuiteSummary(name, tests, failures, errors, skipped, time))

            for case_node in suite_node.findall("testcase"):
                if case_node.find("failure") is not None or case_node.find("error") is not None:
                    case_name = case_node.attrib.get("name", "unknown")
                    class_name = case_node.attrib.get("classname", "")
                    failed_cases.append(f"{class_name}::{case_name}".strip(":"))

    total_tests = sum(item.tests for item in summaries)
    total_failures = sum(item.failures for item in summaries)
    total_errors = sum(item.errors for item in summaries)
    total_skipped = sum(item.skipped for item in summaries)
    total_time = sum(item.time for item in summaries)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    page_width, page_height = A4
    pdf = canvas.Canvas(str(output_path), pagesize=A4)
    y = draw_header(pdf, args.title, page_width, page_height)

    pdf.setFont("Helvetica", 11)
    summary_lines = [
      f"Total tests: {total_tests}",
      f"Failures: {total_failures}",
      f"Errors: {total_errors}",
      f"Skipped: {total_skipped}",
      f"Execution time: {total_time:.2f}s",
    ]
    for line in summary_lines:
        pdf.drawString(20 * mm, y, line)
        y -= 7 * mm

    y -= 2 * mm
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(20 * mm, y, "Suite")
    pdf.drawString(120 * mm, y, "Tests")
    pdf.drawString(140 * mm, y, "Failed")
    pdf.drawString(160 * mm, y, "Skipped")
    pdf.drawString(180 * mm, y, "Time")
    y -= 5 * mm
    pdf.setStrokeColor(colors.lightgrey)
    pdf.line(20 * mm, y, page_width - 20 * mm, y)
    y -= 6 * mm

    for summary in summaries:
        y = ensure_space(pdf, y, 30 * mm, args.title, page_width, page_height)
        pdf.setFont("Helvetica", 9)
        pdf.drawString(20 * mm, y, truncate(summary.name, 95 * mm, "Helvetica", 9))
        pdf.drawRightString(132 * mm, y, str(summary.tests))
        pdf.drawRightString(152 * mm, y, str(summary.failures + summary.errors))
        pdf.drawRightString(172 * mm, y, str(summary.skipped))
        pdf.drawRightString(page_width - 20 * mm, y, f"{summary.time:.2f}s")
        y -= 5 * mm

    if failed_cases:
        y = ensure_space(pdf, y - 2 * mm, 40 * mm, args.title, page_width, page_height)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(20 * mm, y, "Failed Test Cases")
        y -= 7 * mm
        for failed_case in failed_cases[:40]:
            y = ensure_space(pdf, y, 25 * mm, args.title, page_width, page_height)
            pdf.setFont("Helvetica", 9)
            pdf.drawString(20 * mm, y, truncate(failed_case, 170 * mm, "Helvetica", 9))
            y -= 5 * mm

    pdf.save()


if __name__ == "__main__":
    main()
