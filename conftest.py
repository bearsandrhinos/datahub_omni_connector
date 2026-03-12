import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
        help="Re-generate golden files instead of comparing against them.",
    )
