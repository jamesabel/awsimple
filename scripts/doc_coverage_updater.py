from pathlib import Path
from xml.etree import ElementTree

from ismain import is_main


def doc_coverage_updater():
    attributes = ElementTree.parse(Path("cov", "coverage.xml")).getroot().attrib
    numerator = float(attributes["lines-covered"]) + float(attributes["branches-covered"])
    denominator = float(attributes["lines-valid"]) + float(attributes["branches-valid"])
    coverage = numerator/denominator
    Path("doc_source", "coverage.txt").write_text(f"Test coverage: {coverage:.2%}")


if is_main():
    doc_coverage_updater()
