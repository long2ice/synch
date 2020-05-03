from flask import Flask

from mysql2ch.common import redis_ins

app = Flask(__name__)


@app.route('/monitor')
def monitor():
    producer_keys = redis_ins.keys('ui:producer:*')
    consumer_keys = redis_ins.keys('ui:consumer:*')
    ret = {}
    for producer_key in producer_keys:
        ret.setdefault('producer', {}).setdefault(producer_key.split('producer:')[-1], []).append(
            redis_ins.hgetall(producer_key))
    for consumer_key in consumer_keys:
        ret.setdefault('consumer', {}).setdefault(consumer_key.split('consumer:')[-1], []).append(
            redis_ins.hgetall(consumer_key))
    return ret
