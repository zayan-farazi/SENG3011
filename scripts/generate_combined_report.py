#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import xml.etree.ElementTree as ET
from pathlib import Path

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate combined HTML and PDF reports from CI and staging artifacts.")
    parser.add_argument("--ci-dir", required=True, help="Directory containing CI report artifacts")
    parser.add_argument("--staging-dir", required=True, help="Directory containing staging report artifacts")
    parser.add_argument("--output-dir", required=True, help="Directory to write generated reports to")
    return parser.parse_args()


def render_page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>{html.escape(title)}</title>
    <style>
      @page {{ size: A4; margin: 18mm; }}
      body {{ font-family: Helvetica, Arial, sans-serif; color: #1f2937; font-size: 12px; }}
      h1 {{ margin: 0 0 12px; font-size: 24px; }}
      h2 {{ margin: 24px 0 8px; font-size: 16px; }}
      p {{ margin: 6px 0; }}
      table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
      th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: left; vertical-align: top; }}
      th {{ background: #f3f4f6; }}
      .muted {{ color: #6b7280; }}
      .ok {{ color: #166534; }}
      .fail {{ color: #991b1b; }}
      code {{ font-family: "Courier New", monospace; font-size: 11px; }}
    </style>
  </head>
  <body>
    <h1>{html.escape(title)}</h1>
    {body}
  </body>
</html>
"""


def iter_suites(root: ET.Element) -> list[ET.Element]:
    if root.tag == "testsuite":
        return [root]
    if root.tag == "testsuites":
        return list(root.findall("testsuite"))
    return list(root.findall(".//testsuite"))


def build_coverage_report(ci_dir: Path, output_dir: Path) -> None:
    coverage_root = ET.parse(ci_dir / "coverage.xml").getroot()
    total_line_rate = float(coverage_root.attrib.get("line-rate", 0.0)) * 100
    lines_valid = int(coverage_root.attrib.get("lines-valid", 0))
    lines_covered = int(coverage_root.attrib.get("lines-covered", 0))

    coverage_files: list[tuple[str, float, int, int]] = []
    for class_node in coverage_root.findall(".//class"):
        filename = class_node.attrib.get("filename", "unknown")
        rate = float(class_node.attrib.get("line-rate", 0.0)) * 100
        line_nodes = class_node.findall("./lines/line")
        valid = len(line_nodes)
        covered = sum(1 for line in line_nodes if int(line.attrib.get("hits", "0")) > 0)
        coverage_files.append((filename, rate, covered, valid))

    coverage_files.sort(key=lambda item: (item[1], item[0]))
    coverage_rows = "\n".join(
        f"<tr><td><code>{html.escape(filename)}</code></td><td>{rate:.2f}%</td><td>{covered}/{valid}</td></tr>"
        for filename, rate, covered, valid in coverage_files[:25]
    )

    coverage_body = f"""
<p><strong>Overall line coverage:</strong> {total_line_rate:.2f}%</p>
<p><strong>Covered lines:</strong> {lines_covered}/{lines_valid}</p>
<h2>Lowest Coverage Files</h2>
<table>
  <thead>
    <tr><th>File</th><th>Coverage</th><th>Covered / Total</th></tr>
  </thead>
  <tbody>
    {coverage_rows}
  </tbody>
</table>
"""
    (output_dir / "coverage-report.html").write_text(render_page("CI Coverage Report", coverage_body), encoding="utf-8")


def build_test_report(ci_dir: Path, staging_dir: Path, output_dir: Path) -> None:
    junit_inputs = [
        ci_dir / "unit-integration-junit.xml",
        staging_dir / "staging-junit.xml",
        staging_dir / "staging-e2e-junit.xml",
    ]

    suite_rows: list[str] = []
    failed_cases: list[str] = []
    missing_inputs: list[str] = []
    parsed_suites: list[tuple[int, int, int, float]] = []

    for junit_path in junit_inputs:
        if not junit_path.exists():
            missing_inputs.append(str(junit_path))
            continue

        root = ET.parse(junit_path).getroot()
        for suite_node in iter_suites(root):
            suite_name = suite_node.attrib.get("name", junit_path.stem)
            tests = int(suite_node.attrib.get("tests", 0))
            failures = int(suite_node.attrib.get("failures", 0))
            errors = int(suite_node.attrib.get("errors", 0))
            skipped = int(suite_node.attrib.get("skipped", 0))
            duration = float(suite_node.attrib.get("time", 0.0))
            failed = failures + errors
            parsed_suites.append((tests, failed, skipped, duration))
            suite_rows.append(
                f"<tr><td>{html.escape(suite_name)}</td><td>{tests}</td><td class=\"{'fail' if failed else 'ok'}\">{failed}</td><td>{skipped}</td><td>{duration:.2f}s</td><td><code>{html.escape(str(junit_path))}</code></td></tr>"
            )

            for case_node in suite_node.findall("testcase"):
                if case_node.find("failure") is None and case_node.find("error") is None:
                    continue
                case_name = case_node.attrib.get("name", "unknown")
                class_name = case_node.attrib.get("classname", "")
                failed_cases.append(f"{class_name}::{case_name}".strip(":"))

    if not suite_rows:
        raise SystemExit("No JUnit XML files were available to build the combined test report.")

    total_tests = sum(item[0] for item in parsed_suites)
    total_failed = sum(item[1] for item in parsed_suites)
    total_skipped = sum(item[2] for item in parsed_suites)
    total_time = sum(item[3] for item in parsed_suites)

    missing_html = ""
    if missing_inputs:
        missing_items = "".join(f"<li><code>{html.escape(item)}</code></li>" for item in missing_inputs)
        missing_html = f"""
<h2>Missing Inputs</h2>
<p class="muted">These files were not present in the staging or CI artifacts and were skipped.</p>
<ul>
  {missing_items}
</ul>
"""

    failed_cases_html = ""
    if failed_cases:
        failed_case_items = "".join(f"<li><code>{html.escape(item)}</code></li>" for item in failed_cases[:40])
        failed_cases_html = f"""
<h2>Failed Test Cases</h2>
<ul>
  {failed_case_items}
</ul>
"""

    test_body = f"""
<p><strong>Total tests:</strong> {total_tests}</p>
<p><strong>Failures and errors:</strong> <span class="{'fail' if total_failed else 'ok'}">{total_failed}</span></p>
<p><strong>Skipped:</strong> {total_skipped}</p>
<p><strong>Execution time:</strong> {total_time:.2f}s</p>
<h2>Suite Summary</h2>
<table>
  <thead>
    <tr><th>Suite</th><th>Tests</th><th>Failed</th><th>Skipped</th><th>Time</th><th>Source</th></tr>
  </thead>
  <tbody>
    {''.join(suite_rows)}
  </tbody>
</table>
{missing_html}
{failed_cases_html}
"""
    (output_dir / "test-report.html").write_text(render_page("Combined Test Report", test_body), encoding="utf-8")


def main() -> None:
    args = parse_args()
    ci_dir = Path(args.ci_dir)
    staging_dir = Path(args.staging_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    build_coverage_report(ci_dir, output_dir)
    build_test_report(ci_dir, staging_dir, output_dir)


if __name__ == "__main__":
    main()
