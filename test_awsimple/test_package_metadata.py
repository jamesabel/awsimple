"""
Package metadata tests. All tests (and the private helpers) in this file are AI-generated (Claude Code).
"""

import json
import re
import subprocess
import sys
from pathlib import Path

from awsimple.__version__ import __version__, __title__, __author__, __author_email__, __url__, __description__

REPO_ROOT = Path(__file__).parent.parent


def _package_metadata() -> dict:
    """
    The package metadata as the build backend (hatchling) resolves it from pyproject.toml, including the dynamic version.
    """
    result = subprocess.run([sys.executable, "-m", "hatchling", "metadata"], cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8")
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_classifiers():
    """
    The README's PyPI badges (Python versions, typed) are generated from the trove classifiers, so they must be present.
    """
    metadata = _package_metadata()
    classifiers = metadata["classifiers"]
    assert "Typing :: Typed" in classifiers
    assert metadata["license"] == "MIT"  # an SPDX license expression, which replaces the license classifier
    python_versions = [c for c in classifiers if c.startswith("Programming Language :: Python :: 3.")]
    assert len(python_versions) >= 3


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_documentation_url():
    """
    The documentation link shown on PyPI must point at the GitHub Pages site.
    """
    assert _package_metadata()["urls"]["Documentation"] == "https://jamesabel.github.io/awsimple/"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_python_floor():
    """
    The minimum supported Python version must be stated the same way everywhere: Requires-Python, the lowest trove classifier and the lowest version in the CI test matrix.
    """
    metadata = _package_metadata()
    assert metadata["requires-python"] == ">=3.12"
    floor = (3, 12)

    classifier_prefix = "Programming Language :: Python :: 3."
    classifier_versions = sorted((3, int(c.removeprefix(classifier_prefix))) for c in metadata["classifiers"] if c.startswith(classifier_prefix))
    assert classifier_versions[0] == floor

    workflow = (REPO_ROOT / ".github" / "workflows" / "python-package.yml").read_text(encoding="utf-8")
    matrix_match = re.search(r"^\s*python-version: \[(.+)\]", workflow, re.MULTILINE)
    assert matrix_match is not None
    matrix_versions = sorted(tuple(int(part) for part in version.split(".")) for version in re.findall(r'"(\d+\.\d+)"', matrix_match.group(1)))
    assert matrix_versions[0] == floor


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_tox_envlist_matches_ci_matrix():
    """
    The local test run (tox, used by the pre-commit hook) must cover the same Python versions as the CI test matrix.
    """
    workflow = (REPO_ROOT / ".github" / "workflows" / "python-package.yml").read_text(encoding="utf-8")
    matrix_match = re.search(r"^\s*python-version: \[(.+)\]", workflow, re.MULTILINE)
    assert matrix_match is not None
    matrix_versions = re.findall(r'"(\d+\.\d+)"', matrix_match.group(1))

    tox_ini = (REPO_ROOT / "tox.ini").read_text(encoding="utf-8")
    envlist_match = re.search(r"^envlist = (.+)$", tox_ini, re.MULTILINE)
    assert envlist_match is not None
    tox_envs = [env.strip() for env in envlist_match.group(1).split(",")]

    assert tox_envs == ["py" + version.replace(".", "") for version in matrix_versions]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_package_metadata_matches_version_module():
    """
    pyproject.toml takes the version from awsimple/__version__.py, and the values it repeats from that module (which the package also uses at runtime) must agree with it.
    """
    metadata = _package_metadata()
    assert metadata["version"] == __version__
    assert metadata["name"] == __title__
    assert metadata["description"] == __description__
    assert metadata["authors"] == [{"name": __author__, "email": __author_email__}]
    assert metadata["urls"]["Homepage"] == __url__
