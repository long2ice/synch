========
mysql2ch
========

A tool replication data from MySQL to ClickHouse,rewrite from `mysql-clickhouse-replication <https://github.com/yymysql/mysql-clickhouse-replication>`_\ ,
support python3 & pypy.

Installation
============

.. code-block:: bash

    pip3 install mysql2ch

Usage
=====
.. code-block:: bash

   usage: mysql2ch [-h] [-c CONF] [-d] [--log-pos-to {redis,file} | --etl]
                   [--etl-database ETL_DATABASE] [--etl-table ETL_TABLE]
                   [--log-file LOG_FILE]

   A tool replication data from MySQL to ClickHouse

   optional arguments:
     -h, --help            show this help message and exit
     -c CONF, --conf CONF  Data synchronization config file
     -d, --debug           Display SQL information.
     --log-pos-to {redis,file}
                           log position to redis or file.
     --etl                 Full data etl with table create,must create database
                           first.
     --etl-database ETL_DATABASE
                           Database to full etl.
     --etl-table ETL_TABLE
                           Table to full etl.
     --log-file LOG_FILE   logging file.

Config
======

.. code-block:: ini

   # config.ini
   # mysql server
   [master_server]
   host = 127.0.0.1
   port = 3306
   user = root
   password = 
   server_id = 101

   # redis server to record log pos
   [redis_server]
   host = 127.0.0.1
   port = 6379
   password =
   log_pos_prefix = log_pos

   # file to record log pos
   [log_position]
   log_pos_file = ./repl_pos.ini

   # clickhouse config
   [clickhouse_server]
   host = 127.0.0.1
   port = 9000
   password =
   user = default

   # only these schemas to replication
   [only_schemas]
   schemas = database1,database2

   #only these tables to replication
   [only_tables]
   tables = user,order

   # which table to skip delete or update
   [skip_dmls_sing]
   skip_delete_tb_name =
   skip_update_tb_name =

   # skip delete or update
   [skip_dmls_all]
   #skip_type = delete,update
   skip_type =

   [bulk_insert_nums]
   # how many num to submit
   insert_nums = 20000 
   # how many seconds to submit
   interval = 60 

   # email error log notify config
   [failure_alert]
   mail_host = smtp.xx.com
   mail_port = 25
   mail_user = xx
   mail_password = xxx
   mail_send_from = xxx
   mail_send_to = yymysql@gmail.com

If you use file to record log pos:

.. code-block:: ini

   # repl_pos.ini
   [log_position]
   log_file = mysql-bin.000002
   log_pos = 1486731

If you use redis to record log pos,depend on config:

.. code-block:: bash

    hmset '{log_pos_prefix}:{server_id}' log_file 'mysql-bin.000002' log_pos '1486731'

Example
=======

* Full table etl from MySQL to ClickHouse:

.. code-block:: bash

    mysql2ch --etl --etl-database=database --etl-table=table

* Real-time synchronization

.. code-block:: bash

     mysql2ch --log-pos-to=file
     [14:19:42] [INFO]- 开始同步数据时间 2020-02-15 14:19:42
     [14:19:42] [INFO]- 数据库binlog：mysql-bin.000002:1486731
     [14:19:42] [INFO]- 开始同步: help.app_user
     [14:19:42] [INFO]- 开始同步: help.app_task


ThanksTo
========

* `mysql-clickhouse-replication <https://github.com/yymysql/mysql-clickhouse-replication>`_
