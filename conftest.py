import psycopg2
import pytest

from synch.factory import get_reader, get_writer, init
from synch.settings import Settings

alias_postgres = "postgres_db"
alias_mysql = "mysql_db"


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(request):
    init("synch.yaml")


def get_mysql_database():
    return Settings.get_source_db(alias_mysql).get("databases")[0].get("database")


def get_postgres_database():
    return Settings.get_source_db(alias_postgres).get("databases")[0].get("database")


@pytest.fixture(scope="session", autouse=True)
def create_mysql_table(initialize_tests):
    database = get_mysql_database()
    sql = f"""create database if not exists {database};use {database};create table if not exists `test`  (
  `id` int not null auto_increment,
  `amount` decimal(10,2) not null,
  primary key (`id`)
) engine=innodb auto_increment=10 default charset=utf8mb4 collate=utf8mb4_general_ci"""
    reader = get_reader(alias_mysql)
    reader.execute(sql)


@pytest.fixture(scope="session", autouse=True)
def create_postgres_table(initialize_tests):
    database = get_postgres_database()
    reader = get_reader(alias_postgres)
    sql = f"create database {database}"
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


@pytest.fixture(scope="function")
def truncate_postgres_table(request):
    postgres = get_postgres_database()
    sql = f"truncate table {postgres}.public.test restart identity cascade"
    reader = get_reader(alias_postgres)
    reader.execute(sql)

    def finalizer():
        reader.execute(sql)
        get_writer().execute(f"truncate table if exists {postgres}.test")

    request.addfinalizer(finalizer)


@pytest.fixture(scope="function")
def truncate_mysql_table(request):
    database = get_mysql_database()
    sql = f"truncate table {database}.test"
    reader = get_reader(alias_mysql)

    reader.execute(sql)

    def finalizer():
        reader.execute(sql)
        get_writer().execute(f"truncate table if exists {database}.test")

    request.addfinalizer(finalizer)
