#!/usr/bin/env python3
import argparse
import html
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


TRX_NS = {"trx": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}


def first_line(text: str | None, limit: int = 200) -> str:
    if not text:
        return ""
    line = " ".join(text.splitlines()).strip()
    if len(line) <= limit:
        return line
    return line[: limit - 1] + "…"


def parse_trx(path: Path) -> tuple[dict[str, int], list[dict[str, str]]]:
    root = ET.parse(path).getroot()

    definitions: dict[str, dict[str, str]] = {}
    for unit_test in root.findall(".//trx:TestDefinitions/trx:UnitTest", TRX_NS):
        test_id = unit_test.attrib.get("id")
        method = unit_test.find("trx:TestMethod", TRX_NS)
        definitions[test_id or ""] = {
            "name": unit_test.attrib.get("name", ""),
            "class_name": method.attrib.get("className", "") if method is not None else "",
        }

    counters = {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
    }

    counter_node = root.find(".//trx:ResultSummary/trx:Counters", TRX_NS)
    if counter_node is not None:
        counters["total"] = int(counter_node.attrib.get("total", "0"))
        counters["passed"] = int(counter_node.attrib.get("passed", "0"))
        counters["failed"] = int(counter_node.attrib.get("failed", "0"))
        counters["skipped"] = int(counter_node.attrib.get("notExecuted", "0"))

    failures: list[dict[str, str]] = []
    for result in root.findall(".//trx:Results/trx:UnitTestResult", TRX_NS):
        if result.attrib.get("outcome") != "Failed":
            continue

        test_id = result.attrib.get("testId", "")
        test_def = definitions.get(test_id, {})
        test_name = result.attrib.get("testName") or test_def.get("name") or test_id or "Unknown"
        class_name = test_def.get("class_name", "")
        message = result.findtext("trx:Output/trx:ErrorInfo/trx:Message", "", TRX_NS)

        failures.append(
            {
                "suite": root.attrib.get("name", path.stem),
                "test": f"{class_name}.{test_name}".strip("."),
                "message": first_line(message),
            }
        )

    return counters, failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Write a TRX failure summary to GitHub step summary.")
    parser.add_argument("title", help="Section title for the summary")
    parser.add_argument("paths", nargs="+", help="Directories or TRX files to scan")
    args = parser.parse_args()

    trx_files: list[Path] = []
    for raw_path in args.paths:
        path = Path(raw_path)
        if path.is_file() and path.suffix.lower() == ".trx":
            trx_files.append(path)
        elif path.is_dir():
            trx_files.extend(sorted(path.rglob("*.trx")))

    total = {"total": 0, "passed": 0, "failed": 0, "skipped": 0}
    failures: list[dict[str, str]] = []
    for trx_file in trx_files:
        counters, trx_failures = parse_trx(trx_file)
        for key in total:
            total[key] += counters[key]
        failures.extend(trx_failures)

    lines = [f"## {args.title}", ""]
    if not trx_files:
        lines.extend(["No TRX files were found.", ""])
    else:
        lines.extend(
            [
                f"- TRX files: {len(trx_files)}",
                f"- Total: {total['total']}",
                f"- Passed: {total['passed']}",
                f"- Failed: {total['failed']}",
                f"- Skipped: {total['skipped']}",
                "",
            ]
        )

        if failures:
            lines.extend(["### Failing tests", ""])
            for failure in failures[:50]:
                suite = html.escape(failure["suite"])
                test = html.escape(failure["test"])
                message = html.escape(failure["message"])
                line = f"- **{suite}** — `{test}`"
                if message:
                    line += f": {message}"
                lines.append(line)
            if len(failures) > 50:
                lines.append("")
                lines.append(f"_Showing first 50 of {len(failures)} failing tests._")
            lines.append("")
        else:
            lines.extend(["✅ All tests passed.", ""])

    output = "\n".join(lines)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as handle:
            handle.write(output)
            handle.write("\n")
    else:
        print(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
