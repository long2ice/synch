import pytest

from mysql2ch import init


@pytest.fixture(scope="session", autouse=True)
def initialize_tests():
    init('../mysql2ch.ini')
