import pytest

from synch.factory import init


@pytest.fixture(scope="session", autouse=True)
def initialize_tests():
    init('tests/synch.ini')
