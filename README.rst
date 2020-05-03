========
mysql2ch
========

.. image:: https://img.shields.io/pypi/v/mysql2ch.svg?style=flat
   :target: https://pypi.python.org/pypi/mysql2ch
.. image:: https://img.shields.io/docker/cloud/build/long2ice/mysql2ch
   :target: https://hub.docker.com/repository/docker/long2ice/mysql2ch
.. image:: https://img.shields.io/github/license/long2ice/mysql2ch
   :target: https://github.com/long2ice/mysql2ch
.. image:: https://github.com/long2ice/mysql2ch/workflows/pypi/badge.svg
   :target: https://github.com/long2ice/mysql2ch/actions?query=workflow:pypi

Introduction
============

mysql2ch is used to sync data from MySQL to ClickHouse.

.. image:: https://github.com/long2ice/mysql2ch/raw/master/images/mysql2ch.png

Features
========

* Full data etl and continuous sync.
* Support DDL sync,current support ``add column`` and ``drop column``.
* Rich configurable items.

Requirements
============

* `kafka <https://kafka.apache.org>`_,message queue to store mysql binlog event.
* `redis <https://redis.io>`_,cache mysql binlog file and position.

Install
=======

.. code-block:: shell

    $ pip install mysql2ch

.. note::

    Use pypy3 to speed up.

Usage
=====

Make a ``.env`` file in execute dir or set system environment variable:

.env
~~~~

.. code-block:: ini

    # if True,will display sql information
    DEBUG=True
    # sentry need
    ENVIRONMENT=development

    MYSQL_HOST=127.0.0.1
    MYSQL_PORT=3306
    MYSQL_USER=root
    MYSQL_PASSWORD=123456
    MYSQL_SERVER_ID=101

    REDIS_HOST=127.0.0.1
    REDIS_PORT=6379
    REDIS_DB=0

    CLICKHOUSE_HOST=127.0.0.1
    CLICKHOUSE_PORT=9002
    CLICKHOUSE_PASSWORD=
    CLICKHOUSE_USER=default

    SENTRY_DSN=https://3450e192063d47aea7b9733d3d52585f@sentry.test.com/1

    KAFKA_SERVER=127.0.0.1:9092
    KAFKA_TOPIC=mysql2ch

    # kafka partitions mapping,which means binlog of ``test`` will produce to 0 partition.
    SCHEMA_TABLE=test.test;
    PARTITIONS=test=0;

    # init binlog file and position,should set first,after will read from redis.
    INIT_BINLOG_FILE=binlog.000474
    INIT_BINLOG_POS=155

    # how many num to submit
    INSERT_NUMS=20000
    # how many seconds to submit
    INSERT_INTERVAL=60

Full data etl
~~~~~~~~~~~~~

Maybe you need make full data etl before continuous sync data from MySQL to ClickHouse or redo data etl with ``--renew``.

.. code-block:: shell

    $ mysql2ch etl -h

    usage: mysql2ch etl [-h] --schema SCHEMA --tables TABLES [--renew]

    optional arguments:
      -h, --help       show this help message and exit
      --schema SCHEMA  Schema to full etl.
      --tables TABLES  Tables to full etl,multiple tables split with comma.
      --renew          Etl after try to drop the target tables.


Produce
~~~~~~~

Listen all MySQL binlog and produce to kafka.

.. code-block:: shell

    $ mysql2ch produce

Consume
~~~~~~~

Consume message from kafka and insert to ClickHouse,and you can skip error with ``--skip-error``.

.. code-block:: shell

    $ mysql2ch consume -h

    usage: mysql2ch consume [-h] --schema SCHEMA --tables TABLES [--skip-error] --group-id GROUP_ID [--auto-offset-reset AUTO_OFFSET_RESET]

    optional arguments:
      -h, --help            show this help message and exit
      --schema SCHEMA       Schema to consume.
      --skip-error          Skip error rows.
      --auto-offset-reset AUTO_OFFSET_RESET
                            Kafka auto offset reset.

.. note::
    When one service consume multiple partitions,consumer commit maybe incorrect when insert error.

Use docker-compose(recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    version: '3'
    services:
      producer:
        env_file:
          - .env
        depends_on:
          - redis
        image: long2ice/mysql2ch:latest
        command: mysql2ch produce
      # add more service if you need.
      consumer.test:
        env_file:
          - .env
        depends_on:
          - redis
          - producer
        image: long2ice/mysql2ch:latest
        # consume binlog of test
        command: mysql2ch consume --schema test
      redis:
        hostname: redis
        image: redis:latest
        volumes:
          - redis:/data
    volumes:
      redis:

Optional
========

`Sentry <https://github.com/getsentry/sentry>`_,error reporting,worked if set ``SENTRY_DSN`` in ``.env``.

License
=======

This project is licensed under the `MIT <https://github.com/long2ice/mysql2ch/blob/master/LICENSE>`_ License.