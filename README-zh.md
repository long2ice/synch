# Synch

![pypi](https://img.shields.io/pypi/v/synch.svg?style=flat)
![docker](https://img.shields.io/docker/cloud/build/long2ice/synch)
![license](https://img.shields.io/github/license/long2ice/synch)
![workflows](https://github.com/long2ice/synch/workflows/pypi/badge.svg)
![workflows](https://github.com/long2ice/synch/workflows/ci/badge.svg)

[English](https://github.com/long2ice/synch/blob/dev/README.md)

## 简介

从其他数据库同步到 ClickHouse，当前支持 MySQL 与 postgres，支持全量复制与增量复制。

![synch](https://github.com/long2ice/synch/raw/dev/images/synch.png)

## 特性

- 全量复制与实时增量复制。
- 支持 DML 同步与 DDL 同步， 支持增加字段、删除字段、更改字段，并且支持所有的 DML。
- 自定义配置项。
- 支持 redis 与 kafka 作为消息队列。
- 支持多源数据库同时同步到 ClickHouse。

## 依赖

- [redis](https://redis.io)，缓存 binlog 和作为消息队列，支持 redis 集群。
- [kafka](https://kafka.apache.org)，使用 kafka 作为消息队列时需要。
- [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge)， 在 postgres 执行`etl`命令的时候需要。
- [sentry](https://github.com/getsentry/sentry)，可选的，提供错误报告。

## 安装

```shell
> pip install synch
```

## 使用

### synch.yaml

synch 默认从 `./synch.yaml`读取配置， 或者可以使用`synch -c` 指定配置文件。

参考配置文件 [`synch.yaml`](https://github.com/long2ice/synch/blob/dev/synch.yaml)。

### 全量复制

在增量复制之前一般需要进行一次全量复制，或者使用`--renew`进行全量重建。

```shell
> synch etl -h

usage: synch etl [-h] --schema SCHEMA [--tables TABLES] [--renew] [--partition-by PARTITION_BY] [--settings SETTINGS] [--engine ENGINE]

optional arguments:
  -h, --help            show this help message and exit
  --schema SCHEMA       Schema to full etl.
  --tables TABLES       Tables to full etl, multiple tables split with comma.
  --renew               Etl after try to drop the target tables.
  --partition-by PARTITION_BY
                        Table create partitioning by, like toYYYYMM(created_at).
  --settings SETTINGS   Table create settings, like index_granularity=8192
  --engine ENGINE       Table create engine, default MergeTree.

```

全量复制表 `test.test`：

```shell
> synch etl --schema test --tables test
```

### 生产

监听源库并将变动数据写入消息队列。

```shell
> synch produce
```

### 消费

从消息队列中消费数据并插入 ClickHouse，使用 `--skip-error`跳过错误消息。 配置 `auto_full_etl = True` 的时候会首先尝试做一次全量复制。

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

消费数据库 `test` 并插入到`ClickHouse`：

```shell
> synch consume --schema test
```

**一个消费者消费一个数据库产生的消息**

### ClickHouse 表引擎

现在 synch 支持 `MergeTree` 和 `CollapsingMergeTree`，`CollapsingMergeTree` 的性能要高于`MergeTree`。

通常情况下你应该选择`MergeTree`，如果你追求更高性能或者你的数据库极为频繁的更新，你可以选择 `CollapsingMergeTree`， 但是你的 `select` sql 语句需要重写。 更多详情参考[CollapsingMergeTree](https://clickhouse.tech/docs/zh/engines/table-engines/mergetree-family/collapsingmergetree/)。

## 使用 docker-compose（推荐）

<details>
<summary>Redis 作为消息队列，轻量级消息队列，依赖少</summary>

```yaml
version: "3"
services:
  producer:
    depends_on:
      - redis
    image: long2ice/synch
    command: synch produce
    volumes:
      - ./synch.yaml:/synch/synch.yaml
  # 一个消费者消费一个数据库
  consumer.test:
    depends_on:
      - redis
    image: long2ice/synch
    command: synch consume --schema test
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
<summary>Kafka作为消息队列，重量级，高吞吐量</summary>

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
    command: synch produce
    volumes:
      - ./synch.yaml:/synch/synch.yaml
  # 一个消费者消费一个数据库
  consumer.test:
    depends_on:
      - redis
      - kafka
      - zookeeper
    image: long2ice/synch
    command: synch consume --schema test
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

## 重要提示

- synch 不支持复合主键，同步的表必须有主键或唯一键。
- DDL 不支持 postgres.
- Postgres 同步未经过大量测试，生产环境谨慎使用。

## QQ 群

<img width="200" src="https://github.com/long2ice/synch/raw/dev/images/qq_group.png"/>

## 支持这个项目

- 只需点一个 star！
- 加入 QQ 群一起交流。
- 捐赠。

### 支付宝

<img width="200" src="https://github.com/long2ice/synch/raw/dev/images/alipay.jpeg"/>

### 微信

<img width="200" src="https://github.com/long2ice/synch/raw/dev/images/wechatpay.jpeg"/>

### PayPal

捐赠 [paypal](https://www.paypal.me/long2ice) 到我的账号 long2ice.

## 感谢

强大的 Python IDE [Pycharm](https://www.jetbrains.com/pycharm/?from=synch) ，来自 [Jetbrains](https://www.jetbrains.com/?from=synch)。

![jetbrains](https://github.com/long2ice/synch/raw/dev/images/jetbrains.svg)

## 开源许可

本项目遵从 [Apache-2.0](https://github.com/long2ice/synch/blob/master/LICENSE) 开源许可。
