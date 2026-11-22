import sys


def test_python_version_matches_pin():
    # Keeps local, CI and Databricks on the same minor version.
    assert sys.version_info[:2] == (3, 12)
