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

### synch.ini

synch 默认从 `./synch.ini`读取配置， 或者可以使用`synch -c` 指定配置文件。

**不要删除任何配置项，即使并不需要，只需要保持默认值即可**

```ini
[core]
# 设置为True时会打印详细的SQL语句
debug = True
# 当前支持kafka和redis
broker_type = redis
# 源数据库类型，当前支持mysql与postgres
source_db = mysql
# 跳过删除的表，多个以逗号分隔，格式为：schema.table
skip_delete_tables =
# 跳过更新的表，多个以逗号分隔，格式为：schema.table
skip_update_tables =
# 跳过的dml，update 或者 delete，多个以逗号分隔
skip_dmls =
# 多少条事件提交一次，正式环境推荐20000
insert_num = 1
# 多少秒提交一次，正式环境推荐60
insert_interval = 1
# 是否在表不存在时自动全量复制
auto_full_etl = True

[sentry]
# sentry的环境
environment = development
# sentry的dsn配置
dsn =

[redis]
host = 127.0.0.1
port = 6379
password =
db = 0
prefix = synch
# 开启redis哨兵模式
sentinel = False
# redis哨兵地址，多个以逗号分隔
sentinel_hosts = 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002
sentinel_master = master
# redis最为消息队列时的最大长度，多余的会按照FIFO删除
queue_max_len = 200000

[mysql]
server_id = 1
# 可选，为空的时候会自动从 `show master status` 读取
init_binlog_file =
# 可选，为空的时候会自动从 `show master status` 读取
init_binlog_pos =
host = mysql
port = 3306
user = root
password = 123456

# 同步的数据库， 格式为 mysql.schema，每一个数据库对应一个配置块
[mysql.test]
# 同步的表，多个以逗号分隔
tables = test
# 该数据库消费对应的kafka的分区，broker=kafka的时候需要
kafka_partition = 0

# source_db = postgres的时候需要
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

# broker_type=kafka的时候需要
[kafka]
# kafka服务器地址，多个以逗号分隔
servers = 127.0.0.1:9092
topic = synch
```

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
<summary>Kafka作为消息队列，重量级，高吞吐量</summary>

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
