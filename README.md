# mysql2ch

![pypi](https://img.shields.io/pypi/v/mysql2ch.svg?style=flat)
![docker](https://img.shields.io/docker/cloud/build/long2ice/mysql2ch)
![license](https://img.shields.io/github/license/long2ice/mysql2ch)
![workflows](https://github.com/long2ice/mysql2ch/workflows/pypi/badge.svg)

[中文文档](https://blog.long2ice.cn/2020/05/mysql2ch%E4%B8%80%E4%B8%AA%E5%90%8C%E6%AD%A5mysql%E6%95%B0%E6%8D%AE%E5%88%B0clickhouse%E7%9A%84%E9%A1%B9%E7%9B%AE/)

## Introduction

Sync data from MySQL to ClickHouse, support full and increment ETL.

![mysql2ch](https://github.com/long2ice/mysql2ch/raw/dev/images/mysql2ch.png)

## Features

- Full data etl and real time increment etl.
- Support DDL and DML sync, current support `add column` and `drop column` and `change column` of DDL, and full support of DML also.
- Custom configurable items.
- Support kafka and redis as broker.

## Requirements

- [redis](https://redis.io), cache mysql binlog file and position and as broker, support redis cluster also.
- [kafka](https://kafka.apache.org), need if you use kafka as broker.

## Install

```shell
> pip install mysql2ch
```

## Usage

### mysql2ch.ini

mysql2ch will read default config from `./mysql2ch.ini`, or you can use `mysql2ch -c` specify config file.

**Don't delete any section in mysql2ch.ini although you don't need it, just keep default as it.**

```ini
[core]
# when set True, will display sql information.
debug = True
# current support redis and kafka
broker_type = redis
mysql_server_id = 1
# optional, read from `show master status` result if empty
init_binlog_file =
# optional, read from `show master status` result if empty
init_binlog_pos =
# these tables skip delete, multiple separated with comma, format with schema.table
skip_delete_tables =
# these tables skip update, multiple separated with comma, format with schema.table
skip_update_tables =
# skip delete or update dmls, multiple separated with comma, example: delete,update
skip_dmls =
# how many num to submit,recommend set 20000 when production
insert_num = 1
# how many seconds to submit,recommend set 60 when production
insert_interval = 1
# auto do full etl at first when table not exists
auto_full_etl = True

[sentry]
# sentry environment
environment = development
# sentry dsn
dsn = https://xxxxxxxx@sentry.test.com/1

[redis]
host = 127.0.0.1
port = 6379
password =
db = 0
prefix = mysql2ch
# enable redis sentinel
sentinel = false
# redis sentinel hosts,multiple separated with comma
sentinel_hosts = 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002
sentinel_master = master
# stream max len, will delete redundant ones with FIFO
queue_max_len = 200000

[mysql]
host = mysql
port = 3306
user = root
password = 123456

# sync schema, format with mysql.schema, each schema for one section.
[mysql.test]
# multiple separated with comma
tables = test
# kafka partition, need when broker_type=kafka
kafka_partition = 0

[clickhouse]
host = 127.0.0.1
port = 9000
user = default
password =

# need when broker_type=kafka
[kafka]
# kafka servers,multiple separated with comma
servers = 127.0.0.1:9092
topic = mysql2ch
```

### Full data etl

Maybe you need make full data etl before continuous sync data from MySQL to ClickHouse or redo data etl with `--renew`.

```shell
> mysql2ch etl -h

usage: mysql2ch etl [-h] --schema SCHEMA [--tables TABLES] [--renew]

optional arguments:
  -h, --help       show this help message and exit
  --schema SCHEMA  Schema to full etl.
  --tables TABLES  Tables to full etl,multiple tables split with comma,default read from environment.
  --renew          Etl after try to drop the target tables.
```

Full etl from table `test.test`:

```shell
> mysql2ch etl --schema test --tables test
```

### Produce

Listen all MySQL binlog and produce to broker.

```shell
> mysql2ch produce
```

### Consume

Consume message from broker and insert to ClickHouse,and you can skip error rows with `--skip-error`. And mysql2ch will do full etl at first when set `auto_full_etl = True` in `mysql2ch.ini`.

```shell
> mysql2ch consume -h

usage: mysql2ch consume [-h] --schema SCHEMA [--skip-error] [--last-msg-id LAST_MSG_ID]

optional arguments:
  -h, --help            show this help message and exit
  --schema SCHEMA       Schema to consume.
  --skip-error          Skip error rows.
  --last-msg-id LAST_MSG_ID
                        Redis stream last msg id or kafka msg offset, depend on broker_type in config.
```

Consume schema `test` and insert into `ClickHouse`:

```shell
> mysql2ch consume --schema test
```

## Use docker-compose(recommended)

<details>
<summary>Redis Broker, lightweight and for low concurrency</summary>

```yaml
version: "3"
services:
  producer:
    depends_on:
      - redis
    image: long2ice/mysql2ch
    command: mysql2ch produce
    volumes:
      - ./mysql2ch.ini:/mysql2ch/mysql2ch.ini
  consumer.test:
    depends_on:
      - redis
    image: long2ice/mysql2ch
    command: mysql2ch consume --schema test
    volumes:
      - ./mysql2ch.ini:/mysql2ch/mysql2ch.ini
  redis:
    hostname: redis
    image: redis:latest
    volumes:
      - redis
volumes:
  redis:
```

</details>

<details>
<summary>Kafka Broker, for high concurrency</summary>

```yml
version: "3"
services:
  zookeeper:
    image: bitnami/zookeeper:3
    hostname: zookeeper
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    volumes:
      - zookeeper:/bitnami
  kafka:
    image: bitnami/kafka:2
    hostname: kafka
    environment:
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - JMX_PORT=23456
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
    depends_on:
      - zookeeper
    volumes:
      - kafka:/bitnami
  kafka-manager:
    image: hlebalbau/kafka-manager
    ports:
      - "9000:9000"
    environment:
      ZK_HOSTS: "zookeeper:2181"
      KAFKA_MANAGER_AUTH_ENABLED: "false"
    command: -Dpidfile.path=/dev/null
  producer:
    depends_on:
      - redis
      - kafka
      - zookeeper
    image: long2ice/mysql2ch
    command: mysql2ch produce
    volumes:
      - ./mysql2ch.ini:/mysql2ch/mysql2ch.ini
  consumer.test:
    depends_on:
      - redis
      - kafka
      - zookeeper
    image: long2ice/mysql2ch
    command: mysql2ch consume --schema test
    volumes:
      - ./mysql2ch.ini:/mysql2ch/mysql2ch.ini
  redis:
    hostname: redis
    image: redis:latest
    volumes:
      - redis:/data
volumes:
  redis:
  kafka:
  zookeeper:
```

</details>

## Limitions

- mysql2ch don't support composite primary key, you need always keep a primary key or unique key.
- mysql2ch will not support table mapping or column mapping, it aims to make clickhouse as mirror database for mysql, and with realtime replication.

## Optional

[Sentry](https://github.com/getsentry/sentry), error reporting, worked if set `dsn` in config.

## QQ Group

<img width="200" src="https://github.com/long2ice/mysql2ch/raw/dev/images/qq_group.png"/>

## ThanksTo

Powerful Python IDE [Pycharm](https://www.jetbrains.com/pycharm/?from=mysql2ch) from [Jetbrains](https://www.jetbrains.com/?from=mysql2ch).

![jetbrains](https://github.com/long2ice/mysql2ch/raw/dev/images/jetbrains.svg)

## License

This project is licensed under the [MIT](https://github.com/long2ice/mysql2ch/blob/master/LICENSE) License.
