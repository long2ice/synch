# ChangeLog

## 0.7

## 0.7.1

- Catch query parse error.
- Support ClickHouse cluster and `synch.yaml` struct changed.

## 0.7.0

- Make DDL sync work better.

## 0.6

### 0.6.9

- Add count in consumer.
- Add more log.
- `synch` table add `id` column.
- Fix insert interval.
- Fix pk with not int type.

### 0.6.8

- Fix json field encode.
- Make `ReplacingMergeTree` as default.
- Add cli `check` to check count of source table and target table.

### 0.6.7

- Fix monitor config.
- Add `ReplacingMergeTree` support.

### 0.6.6

- Add `monitoring` in config, which insert monitoring records in ClickHouse.
- Add email error report config.
- Bug fix.
- Support postgres full etl, need [clickhouse-jdbc-bridge](https://github.com/long2ice/clickhouse-jdbc-bridge).

### 0.6.5

- Fix bugs in skip database table.
- Fix composite primary key etl.

### 0.6.4

- Add `VersionedCollapsingMergeTree` support.
- Add `skip_decimal` in config to fix [#7690](https://github.com/ClickHouse/ClickHouse/issues/7690).
- Add `auto_create` in config.

### 0.6.3

- Fix redis sentinel error.
- Fix debug error.
- Fix delete pk error.

### 0.6.2

- Refactor ClickHouse writer.
- Add `CollapsingMergeTree` support.
- Refactor config file.

### 0.6.1

- Add more args in `etl`.
- Update etl algorithm prevent stuck since big data.

### 0.6.0

- Add support to postgres.
- Config file update, see detail in `synch.ini`.
- Rename project name to `synch`.

## 0.5

### 0.5.4

- Fix sql covert to clickhouse.
- Fix delete bug.

### 0.5.3

- Fix full data etl with composite primary key and skip when no primary key.
- Move `queue_max_len` to redis section.
- Add `debug` in `synch.ini` and remove from cli.
- Add `auto_full_etl` in `synch.ini`.

### 0.5.2

- Fix insert interval block.
- Add kafka broker.
- Add signal to handle stop.

### 0.5.1

- Add queue_max_len config.
- Enable redis sentinel.
- Add support `change column`.

### 0.5.0

- Remove kafka and instead of redis.

## 0.4

### 0.4.5

- Fix DDL sync.
- Fix PK change error.

### 0.4.4

- Support kafka consume offset.
- Fix table convert.

### 0.4.3

- Deep support ddl.

### 0.4.0

- Most of the rewrite.
- Remove read config from env,instead of config.json.
- Remove ui module.
