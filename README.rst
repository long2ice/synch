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


`中文文档 <https://blog.long2ice.cn/2020/05/mysql2ch%E4%B8%80%E4%B8%AA%E5%90%8C%E6%AD%A5mysql%E6%95%B0%E6%8D%AE%E5%88%B0clickhouse%E7%9A%84%E9%A1%B9%E7%9B%AE/>`_


Introduction
============

mysql2ch to sync data from MySQL to ClickHouse, support full and increment.

.. image:: https://github.com/long2ice/mysql2ch/raw/master/images/mysql2ch.png

Features
========

* Full data etl and continuous sync.
* Support DDL and DML sync,current support ``add column`` and ``drop column`` of DDL, and full support of DML also.
* Rich configurable items.
* Consumer and producer monitor ui.

Requirements
============

* `kafka <https://kafka.apache.org>`_,message queue to store mysql binlog event.
* `redis <https://redis.io>`_,cache mysql binlog file and position and store monitor data.

Install
=======

.. code-block:: shell

    $ pip install mysql2ch

Usage
=====

Make a ``.env`` file in execute dir or set system environment variable:

Create .env
~~~~~~~~~~~

Example `.env <https://github.com/long2ice/mysql2ch/blob/master/.env.example>`_.

Full data etl
~~~~~~~~~~~~~

Maybe you need make full data etl before continuous sync data from MySQL to ClickHouse or redo data etl with ``--renew``.

.. code-block:: shell

    $ mysql2ch etl -h

    usage: mysql2ch etl [-h] --schema SCHEMA [--tables TABLES] [--renew]

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

    usage: mysql2ch consume [-h] --schema SCHEMA [--skip-error] [--auto-offset-reset AUTO_OFFSET_RESET]

    optional arguments:
      -h, --help            show this help message and exit
      --schema SCHEMA       Schema to consume.
      --skip-error          Skip error rows.
      --auto-offset-reset AUTO_OFFSET_RESET
                            Kafka auto offset reset,default earliest.

Monitor UI
~~~~~~~~~~

.. code-block:: shell

    $ mysql2ch ui -h

    usage: mysql2ch ui [-h] [--host HOST] [-p PORT]

    optional arguments:
      -h, --help            show this help message and exit
      --host HOST           Listen host.
      -p PORT, --port PORT  Listen port.

Use docker-compose(recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example `docker-compose.yml <https://github.com/long2ice/mysql2ch/blob/master/docker-compose.yml>`_.

Optional
========

`Sentry <https://github.com/getsentry/sentry>`_,error reporting,worked if set ``SENTRY_DSN`` in ``.env``.

License
=======

This project is licensed under the `MIT <https://github.com/long2ice/mysql2ch/blob/master/LICENSE>`_ License.
