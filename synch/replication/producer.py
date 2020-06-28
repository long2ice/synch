import datetime
import logging

from synch.factory import Global

logger = logging.getLogger("synch.producer")


def produce(args):
    reader = Global.reader
    broker = Global.broker
    logger.info(f"start producer success")
    logger.info("start sync at %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    reader.start_sync(broker)
