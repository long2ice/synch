========
mysql2ch
========

.. image:: https://travis-ci.com/long2ice/mysql2ch.svg?branch=master
   :target: https://travis-ci.com/long2ice/mysql2ch
.. image:: https://img.shields.io/pypi/v/mysql2ch.svg?style=flat
   :target: https://pypi.python.org/pypi/mysql2ch
.. image:: https://img.shields.io/docker/cloud/build/long2ice/mysql2ch
   :target: https://hub.docker.com/repository/docker/long2ice/mysql2ch
.. image:: https://img.shields.io/github/license/long2ice/mysql2ch
   :target: https://github.com/long2ice/mysql2ch

Introduction
============

mysql2ch is used to sync data from MySQL to ClickHouse.

.. image:: https://github.com/long2ice/mysql2ch/raw/master/images/mysql2ch.png

Requirements
============

* `kafka <https://kafka.apache.org/>`_
* ``docker`` & ``docker-compose``

Usage
=====

Full data etl
~~~~~~~~~~~~~

.. code-block:: shell

    $ mysql2ch etl -h

    usage: mysql2ch etl [-h] --schema SCHEMA --tables TABLES [--renew]

    optional arguments:
      -h, --help       show this help message and exit
      --schema SCHEMA  Schema to full etl.
      --tables TABLES  Tables to full etl,multiple tables split with comma.
      --renew          Etl after try to drop the target tables.


Continuous Sync
~~~~~~~~~~~~~~~

1. ``cp .env.example .env`` and edit it.
2. edit ``docker-compose.yml``,which will read ``.env``,add your own consumer services in ``docker-compose.yml``.One consumer consume one kafka partition.
3. ``docker-compose up -d``.

.. note::
    When one service consume multiple partitions,consumer commit maybe incorrect when insert error.

Optional
========

`Sentry <https://github.com/getsentry/sentry>`_,error reporting,worked if set ``SENTRY_DSN`` in ``.env``.

Example
=======

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

    # kafka partitions mapping,which means binlog of test.test will produce to 0 partition.
    PARTITIONS=test.test=0;test.test2=1;

    # init binlog file and position,should set first,after will read from redis.
    INIT_BINLOG_FILE=binlog.000474
    INIT_BINLOG_POS=155

    # how many num to submit
    INSERT_NUMS=20000
    # how many seconds to submit
    INSERT_INTERVAL=60

docker-compose.yml
~~~~~~~~~~~~~~~~~~

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
      consumer.test.test:
        env_file:
          - .env
        depends_on:
          - redis
          - producer
        image: long2ice/mysql2ch:latest
        # consume binlog of test.test
        command: mysql2ch consume --schema test --tables test
      redis:
        hostname: redis
        image: redis:latest
        volumes:
          - redis:/data
    volumes:
      redis:
