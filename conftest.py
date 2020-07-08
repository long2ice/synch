import os

import psycopg2
import pytest

from synch.factory import get_reader, get_writer, init

local = os.getenv("local") == "True"


@pytest.fixture(scope="session", autouse=True)
def initialize_tests():
    if local:
        init("synch.yaml")
    else:
        init("tests/synch.yaml")


@pytest.fixture(scope="session", autouse=True)
def create_mysql_table(initialize_tests):
    sql = """drop database if exists test;create database if not exists test;use test;create table if not exists `test` (
  `id` int not null auto_increment,
  `amount` decimal(10,2) default null,
  primary key (`id`)
) engine=innodb auto_increment=10 default charset=utf8mb4 collate=utf8mb4_general_ci"""
    return get_reader("mysql_db").execute(sql)


@pytest.fixture(scope="session", autouse=True)
def create_postgres_table(initialize_tests):
    reader = get_reader("postgres_db")
    sql = "create database test"
    try:
        reader.execute(sql)
    except psycopg2.errors.DuplicateDatabase:
        pass

    sql = """create table if not exists test
(
    id     int not null primary key,
    amount decimal(10, 2) default null
)"""
    try:
        reader.execute(sql)
    except psycopg2.ProgrammingError as e:
        assert str(e) == "no results to fetch"  # nosec: B101


@pytest.fixture(scope="session", autouse=True)
def create_clickhouse_table(initialize_tests):
    sql_create_database = "create database if not exists test"
    sql_create_table = """CREATE TABLE if not exists test.test(`id` Int32, `amount` Nullable(Decimal(10, 2))) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;"""
    writer = get_writer()
    writer.execute(sql_create_database)
    writer.execute(sql_create_table)
