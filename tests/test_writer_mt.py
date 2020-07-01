import pytest

from synch.factory import Global
from synch.writer.merge_tree import ClickHouseMergeTree


@pytest.mark.skip
def test_delete_events():
    ch = ClickHouseMergeTree(Global.settings, Global.broker)
    sql = ch.delete_events("test", "test", "id", [1])
    assert sql == "alter table test.test delete where id in (1)"
    sql = ch.delete_events("test", "test", ("id", "name"), [(1, 2), (2, 3)])
    assert sql == "alter table test.test delete where (id=1 and name=2) or (id=2 and name=3)"
