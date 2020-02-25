import datetime
import json
import logging
import logging.handlers
import sys

from mysql2ch.config import Config


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def init_logging(log_file, debug):
    logger = logging.getLogger("mysql2ch")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        fmt="[%(asctime)s] [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s",
        datefmt="%a %d %b %Y %H:%M:%S"
    ))

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s]- %(message)s",
        datefmt="%H:%M:%S"
    ))

    failure_alert = Config.get('failure_alert')
    eh = logging.handlers.SMTPHandler(
        mailhost=failure_alert.get('mail_host'),
        fromaddr=failure_alert.get('mail_send_from'),
        toaddrs=[failure_alert.get('mail_send_to')],
        subject='mysql2ch错误通知',
        credentials=(failure_alert.get('mail_user'), failure_alert.get('mail_password')),
    )
    eh.setLevel(logging.ERROR)

    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.addHandler(eh)


__version__ = '0.0.1'
