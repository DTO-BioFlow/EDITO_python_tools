from importlib.metadata import PackageNotFoundError, version

import pytest


def test_print_installed_version() -> None:
    """Print installed distribution version for dtotools."""
    try:
        installed_version = version("dtotools")
    except PackageNotFoundError:
        pytest.skip("dtotools is not installed in this environment")

    print(f"Installed dtotools version: {installed_version}")
    assert installed_version

