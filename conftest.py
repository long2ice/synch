import pytest

from mysql2ch.factory import init


@pytest.fixture(scope="session", autouse=True)
def initialize_tests():
    init('./mysql2ch.ini')
