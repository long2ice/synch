from flask import Flask, render_template

from mysql2ch.common import redis_ins, get_chart_data

app = Flask(__name__)


@app.route('/monitor')
def monitor():
    producer_keys = redis_ins.keys('ui:producer:*')
    consumer_keys = redis_ins.keys('ui:consumer:*')
    p_x_axis, p_legend, p_series = get_chart_data('producer', producer_keys)
    c_x_axis, c_legend, c_series = get_chart_data('consumer', consumer_keys)

    return dict(producer=dict(x_axis=p_x_axis, series=p_series, legend=p_legend),
                consumer=dict(x_axis=c_x_axis, series=c_series, legend=c_legend))


@app.route('/')
def index():
    return render_template('monitor.html')
