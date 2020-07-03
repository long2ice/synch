import logging

from synch.writer import ClickHouse

logger = logging.getLogger("synch.writer.collapsing_merge_tree")


class ClickHouseCollapsingMergeTree(ClickHouse):
    pass
