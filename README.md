# Synch

![pypi](https://img.shields.io/pypi/v/synch.svg?style=flat)
![docker](https://img.shields.io/docker/cloud/build/long2ice/synch)
![license](https://img.shields.io/github/license/long2ice/synch)
![workflows](https://github.com/long2ice/synch/workflows/pypi/badge.svg)

[中文文档](https://blog.long2ice.cn/2020/05/synch%E4%B8%80%E4%B8%AA%E5%90%8C%E6%AD%A5mysql%E6%95%B0%E6%8D%AE%E5%88%B0clickhouse%E7%9A%84%E9%A1%B9%E7%9B%AE/)

## Introduction

Sync data from other DB to ClickHouse, current support postgres and mysql, and support full and increment ETL.

![synch](https://github.com/long2ice/synch/raw/dev/images/synch.png)

## Features

- Full data etl and real time increment etl.
- Support DDL and DML sync, current support `add column` and `drop column` and `change column` of DDL, and full support of DML also.
- Custom configurable items.
- Support kafka and redis as broker.

## Requirements

- [redis](https://redis.io), cache mysql binlog file and position and as broker, support redis cluster also.
- [kafka](https://kafka.apache.org), need if you use kafka as broker.
- [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge), need if you use postgres and set `auto_full_etl = True`, or exec `synch etl` command.

## Install

```shell
> pip install synch
```

## Usage

### synch.ini

synch will read default config from `./synch.ini`, or you can use `synch -c` specify config file.

**Don't delete any section in synch.ini although you don't need it, just keep default as it.**

```ini
[core]
# when set True, will display sql information.
debug = True
# current support redis and kafka
broker_type = redis
# source database, current support mysql and postgres
source_db = mysql
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
dsn =

[redis]
host = 127.0.0.1
port = 6379
password =
db = 0
prefix = synch
# enable redis sentinel
sentinel = False
# redis sentinel hosts,multiple separated with comma
sentinel_hosts = 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002
sentinel_master = master
# stream max len, will delete redundant ones with FIFO
queue_max_len = 200000

[mysql]
server_id = 1
# optional, read from `show master status` result if empty
init_binlog_file =
# optional, read from `show master status` result if empty
init_binlog_pos =
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

# when source_db = postgres
[postgres]
host = postgres
port = 5432
user = postgres
password =

[postgres.postgres]
tables = test
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
topic = synch
```

### Full data etl

Maybe you need make full data etl before continuous sync data from MySQL to ClickHouse or redo data etl with `--renew`.

```shell
> synch etl -h

usage: synch etl [-h] --schema SCHEMA [--tables TABLES] [--renew]

optional arguments:
  -h, --help       show this help message and exit
  --schema SCHEMA  Schema to full etl.
  --tables TABLES  Tables to full etl,multiple tables split with comma,default read from environment.
  --renew          Etl after try to drop the target tables.
```

Full etl from table `test.test`:

```shell
> synch etl --schema test --tables test
```

### Produce

Listen all MySQL binlog and produce to broker.

```shell
> synch produce
```

### Consume

Consume message from broker and insert to ClickHouse,and you can skip error rows with `--skip-error`. And synch will do full etl at first when set `auto_full_etl = True` in `synch.ini`.

```shell
> synch consume -h

usage: synch consume [-h] --schema SCHEMA [--skip-error] [--last-msg-id LAST_MSG_ID]

optional arguments:
  -h, --help            show this help message and exit
  --schema SCHEMA       Schema to consume.
  --skip-error          Skip error rows.
  --last-msg-id LAST_MSG_ID
                        Redis stream last msg id or kafka msg offset, depend on broker_type in config.
```

Consume schema `test` and insert into `ClickHouse`:

```shell
> synch consume --schema test
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
    image: long2ice/synch
    command: synch produce
    volumes:
      - ./synch.ini:/synch/synch.ini
  consumer.test:
    depends_on:
      - redis
    image: long2ice/synch
    command: synch consume --schema test
    volumes:
      - ./synch.ini:/synch/synch.ini
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
    image: long2ice/synch
    command: synch produce
    volumes:
      - ./synch.ini:/synch/synch.ini
  consumer.test:
    depends_on:
      - redis
      - kafka
      - zookeeper
    image: long2ice/synch
    command: synch consume --schema test
    volumes:
      - ./synch.ini:/synch/synch.ini
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

## Important

- Synch don't support composite primary key, and you need always keep a primary key or unique key.
- DDL sync not support postgres.
- Postgres sync is not fully test, be careful use it in production.

## Optional

[Sentry](https://github.com/getsentry/sentry), error reporting, worked if set `dsn` in config.

## QQ Group

<img width="200" src="https://github.com/long2ice/synch/raw/dev/images/qq_group.png"/>

## Support this project

- Just give a star!
- Join QQ group for communication.
- Donation.

### AliPay

<img width="200" src="https://github.com/long2ice/synch/raw/dev/images/alipay.jpeg"/>

### WeChat Pay

<img width="200" src="https://github.com/long2ice/synch/raw/dev/images/wechatpay.jpeg"/>

### PayPal

Donate money by [paypal](https://www.paypal.me/long2ice) to my account long2ice.

## ThanksTo

Powerful Python IDE [Pycharm](https://www.jetbrains.com/pycharm/?from=synch) from [Jetbrains](https://www.jetbrains.com/?from=synch).

![jetbrains](https://github.com/long2ice/synch/raw/dev/images/jetbrains.svg)

## License

This project is licensed under the [Apache-2.0](https://github.com/long2ice/synch/blob/master/LICENSE) License.
