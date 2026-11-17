"""
Package metadata tests. All tests in this file are AI-generated (Claude Code).
"""

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_classifiers():
    """
    The README's PyPI badges (Python versions, typed) are generated from the trove classifiers, so they must be present.
    """
    result = subprocess.run([sys.executable, "setup.py", "--classifiers"], cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    classifiers = result.stdout.splitlines()
    assert "Typing :: Typed" in classifiers
    assert "License :: OSI Approved :: MIT License" in classifiers
    python_versions = [c for c in classifiers if c.startswith("Programming Language :: Python :: 3.")]
    assert len(python_versions) >= 3


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_documentation_url(tmp_path):
    """
    The documentation link shown on PyPI must point at the GitHub Pages site.
    """
    result = subprocess.run([sys.executable, "setup.py", "--quiet", "dist_info", "--output-dir", str(tmp_path)], cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    metadata_files = list(tmp_path.glob("*.dist-info/METADATA"))
    assert len(metadata_files) == 1
    assert "Project-URL: Documentation, https://jamesabel.github.io/awsimple/" in metadata_files[0].read_text(encoding="utf-8").splitlines()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_python_floor(tmp_path):
    """
    The minimum supported Python version must be stated the same way everywhere: Requires-Python, the lowest trove classifier and the lowest version in the CI test matrix.
    """
    result = subprocess.run([sys.executable, "setup.py", "--quiet", "dist_info", "--output-dir", str(tmp_path)], cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    metadata_lines = next(tmp_path.glob("*.dist-info/METADATA")).read_text(encoding="utf-8").splitlines()

    requires_python = [line.split(": ", 1)[1] for line in metadata_lines if line.startswith("Requires-Python: ")]
    assert requires_python == [">=3.12"]
    floor = (3, 12)

    classifier_prefix = "Classifier: Programming Language :: Python :: 3."
    classifier_versions = sorted((3, int(line.removeprefix(classifier_prefix))) for line in metadata_lines if line.startswith(classifier_prefix))
    assert classifier_versions[0] == floor

    workflow = (REPO_ROOT / ".github" / "workflows" / "python-package.yml").read_text(encoding="utf-8")
    matrix_match = re.search(r"^\s*python-version: \[(.+)\]", workflow, re.MULTILINE)
    assert matrix_match is not None
    matrix_versions = sorted(tuple(int(part) for part in version.split(".")) for version in re.findall(r'"(\d+\.\d+)"', matrix_match.group(1)))
    assert matrix_versions[0] == floor
