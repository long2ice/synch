# ChangeLog

## 0.6

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
