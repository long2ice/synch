# Synch

![pypi](https://img.shields.io/pypi/v/synch.svg?style=flat)
![docker](https://img.shields.io/docker/cloud/build/long2ice/synch)
![license](https://img.shields.io/github/license/long2ice/synch)
![workflows](https://github.com/long2ice/synch/workflows/pypi/badge.svg)
![workflows](https://github.com/long2ice/synch/workflows/ci/badge.svg)

[中文文档](https://github.com/long2ice/synch/blob/dev/README-zh.md)

## Introduction

Sync data from other DB to ClickHouse, current support postgres and mysql, and support full and increment ETL.

![synch](https://github.com/long2ice/synch/raw/dev/images/synch.png)

## Features

- Full data etl and real time increment etl.
- Support DDL and DML sync, current support `add column` and `drop column` and `change column` of DDL, and full support
  of DML also.
- Email error report.
- Support kafka and redis as broker.
- Multiple source db sync to ClickHouse at the same time。
- Support ClickHouse `MergeTree`,`CollapsingMergeTree`,`VersionedCollapsingMergeTree`,`ReplacingMergeTree`.
- Support ClickHouse cluster.

## Requirements

- Python >= 3.7
- [redis](https://redis.io), cache mysql binlog file and position and as broker, support redis cluster also.
- [kafka](https://kafka.apache.org), need if you use kafka as broker.
- [clickhouse-jdbc-bridge](https://github.com/long2ice/clickhouse-jdbc-bridge), need if you use postgres and
  set `auto_full_etl = true`, or exec `synch etl` command.

## Install

```shell
> pip install synch
```

## Usage

### Config file `synch.yaml`

synch will read default config from `./synch.yaml`, or you can use `synch -c` specify config file.

See full example config in [`synch.yaml`](https://github.com/long2ice/synch/blob/dev/synch.yaml).

### Full data etl

Maybe you need make full data etl before continuous sync data from MySQL to ClickHouse or redo data etl with `--renew`.

```shell
> synch --alias mysql_db etl -h

Usage: synch etl [OPTIONS]

  Make etl from source table to ClickHouse.

Options:
  --schema TEXT     Schema to full etl.
  --renew           Etl after try to drop the target tables.
  -t, --table TEXT  Tables to full etl.
  -h, --help        Show this message and exit.
```

Full etl from table `test.test`:

```shell
> synch --alias mysql_db etl --schema test --table test --table test2
```

### Produce

Listen all MySQL binlog and produce to broker.

```shell
> synch --alias mysql_db produce
```

### Consume

Consume message from broker and insert to ClickHouse,and you can skip error rows with `--skip-error`. And synch will do
full etl at first when set `auto_full_etl = true` in config.

```shell
> synch --alias mysql_db consume -h

Usage: synch consume [OPTIONS]

  Consume from broker and insert into ClickHouse.

Options:
  --schema TEXT       Schema to consume.  [required]
  --skip-error        Skip error rows.
  --last-msg-id TEXT  Redis stream last msg id or kafka msg offset, depend on
                      broker_type in config.

  -h, --help          Show this message and exit.
```

Consume schema `test` and insert into `ClickHouse`:

```shell
> synch --alias mysql_db consume --schema test
```

### Monitor

Set `true` to `core.monitoring`, which will create database `synch` in `ClickHouse` automatically and insert monitoring
data.

Table struct:

```sql
create table if not exists synch.log
(
    alias String,
    schema String,
    table String,
    num        int,
    type       int, -- 1:producer, 2:consumer
    created_at DateTime
)
    engine = MergeTree partition by toYYYYMM
(
    created_at
) order by created_at;
```

### ClickHouse Table Engine

Now synch support `MergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `ReplacingMergeTree`.

- `MergeTree`, default common choices.
- `CollapsingMergeTree`, see detail
  in [CollapsingMergeTree](https://clickhouse.tech/docs/zh/engines/table-engines/mergetree-family/collapsingmergetree/).
- `VersionedCollapsingMergeTree`, see detail
  in [VersionedCollapsingMergeTree](https://clickhouse.tech/docs/zh/engines/table-engines/mergetree-family/versionedcollapsingmergetree/)
  .
- `ReplacingMergeTree`, see detail
  in [ReplacingMergeTree](https://clickhouse.tech/docs/zh/engines/table-engines/mergetree-family/replacingmergetree/).

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
    command: synch --alias mysql_db produce
    volumes:
      - ./synch.yaml:/synch/synch.yaml
  # one service consume on schema
  consumer.test:
    depends_on:
      - redis
    image: long2ice/synch
    command: synch --alias mysql_db consume --schema test
    volumes:
      - ./synch.yaml:/synch/synch.yaml
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

```yaml
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
    command: synch --alias mysql_db produce
    volumes:
      - ./synch.yaml:/synch/synch.yaml
  # one service consume on schema
  consumer.test:
    depends_on:
      - redis
      - kafka
      - zookeeper
    image: long2ice/synch
    command: synch --alias mysql_db consume --schema test
    volumes:
      - ./synch.yaml:/synch/synch.yaml
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

- You need always keep a primary key or unique key without null or composite primary key.
- DDL sync not support postgres.
- Postgres sync is not fully test, be careful use it in production.

## ThanksTo

Powerful Python IDE [Pycharm](https://www.jetbrains.com/pycharm/?from=synch)
from [Jetbrains](https://www.jetbrains.com/?from=synch).

![jetbrains](https://github.com/long2ice/synch/raw/dev/images/jetbrains.svg)

## License

This project is licensed under the [Apache-2.0](https://github.com/long2ice/synch/blob/master/LICENSE) License.
