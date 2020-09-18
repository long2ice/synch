import datetime
import json
import logging
from decimal import Decimal

import dateutil.parser

from synch.settings import Settings

logger = logging.getLogger("synch.common")

CONVERTERS = {
    "date": dateutil.parser.parse,
    "datetime": dateutil.parser.parse,
    "decimal": Decimal,
}


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return {"val": obj.strftime("%Y-%m-%d %H:%M:%S"), "_spec_type": "datetime"}
        elif isinstance(obj, datetime.date):
            return {"val": obj.strftime("%Y-%m-%d"), "_spec_type": "date"}
        elif isinstance(obj, Decimal):
            return {"val": str(obj), "_spec_type": "decimal"}
        else:
            return super().default(obj)


def object_hook(obj):
    _spec_type = obj.get("_spec_type")
    if not _spec_type:
        return obj

    if _spec_type in CONVERTERS:
        return CONVERTERS[_spec_type](obj["val"])
    else:
        raise TypeError("Unknown {}".format(_spec_type))


def insert_log(
    alias: str, schema: str, table: str, num: int, type_: int,
):
    if not Settings.monitoring():
        return
    from synch.factory import get_writer

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""INSERT INTO synch.log (alias, schema, table, num, type, created_at) VALUES ('{alias}', '{schema}', '{table}', {num}, {type_}, '{now}')"""
    get_writer().execute(sql)


def cluster_sql(cluster_name: str = None):
    if cluster_name:
        return f" on cluster {cluster_name} "
    return ""
