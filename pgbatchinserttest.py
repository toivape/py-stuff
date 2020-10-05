import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import csv
from timeit import default_timer as timer


def create_database():
    print('Creating database')
    conn = psycopg2.connect("host='localhost' user='postgres' password='postgres'")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute('DROP DATABASE IF EXISTS benchmark;')
    cur.execute('CREATE DATABASE benchmark;')
    cur.close()


def bench_db():
    return psycopg2.connect("host='localhost' dbname='benchmark' user='postgres' password='postgres'")


def create_benchmark_tables():
    conn = bench_db()
    b1 = """
    CREATE TABLE bench1 (
            id                  SERIAL PRIMARY KEY,
            street              TEXT,
            house_number        TEXT,
            postal_code         TEXT,
            latitude_wgs84      NUMERIC(8,6),
            longitude_wgs84     NUMERIC(8,6)
    );
    """

    b2 = """
    CREATE TABLE bench2 (
            id                  SERIAL PRIMARY KEY,
            street              TEXT,
            house_number        TEXT,
            postal_code         TEXT,
            latitude_wgs84      NUMERIC(8,6),
            longitude_wgs84     NUMERIC(8,6)
    );
    """

    b3 = """
    CREATE TABLE bench3 (
            id                  SERIAL PRIMARY KEY,
            street              TEXT,
            house_number        TEXT,
            postal_code         TEXT,
            latitude_wgs84      NUMERIC(8,6),
            longitude_wgs84     NUMERIC(8,6)
    );
    """

    cur = conn.cursor()
    cur.execute(b1)
    cur.execute(b2)
    cur.execute(b3)
    conn.commit()
    cur.close()
    print('Tables created')


def read_address_data(store_batch):
    num_rows = 0
    with open('./address_coordinates.csv', encoding='ISO-8859-1') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        batch = []
        is_first_row = True
        for row in reader:
            if is_first_row:
                is_first_row = False
                continue
            record = (row[3], row[4], row[5], row[6], row[7])
            batch.append(record)
            num_rows += 1
            if len(batch) > 999:
                store_batch(batch)
                batch = []

    if len(batch) != 0:
        store_batch(batch)

    return num_rows


def print_batch(batch):
    print('***** Batch *****')
    for r in batch:
        print(r)


def insert_executemany(batch):
    conn = bench_db()
    cur = conn.cursor()
    sql = "insert into bench3 (street, house_number, postal_code, latitude_wgs84, longitude_wgs84) values  (%s, %s, %s, %s, %s)"
    cur.executemany(sql, batch)
    conn.commit()
    cur.close()


def just_inserts(batch):
    sql = "insert into bench1 (street, house_number, postal_code, latitude_wgs84, longitude_wgs84) values (%s, %s, %s, %s, %s)"
    conn = bench_db()
    cur = conn.cursor()
    for r in batch:
        cur.execute(sql, r)
    conn.commit()
    cur.close()


def insert_execute_values(batch):
    conn = bench_db()
    cur = conn.cursor()
    sql = "insert into bench2 (street, house_number, postal_code, latitude_wgs84, longitude_wgs84) values  %s"
    execute_values(cur, sql, batch)
    conn.commit()
    cur.close()


def time_me(name, batch_handler):
    start = timer()
    num_rows = read_address_data(batch_handler)
    end = timer()
    print('TIMED ROWS', num_rows, 'IN SECS:', name, end - start)


create_database()
create_benchmark_tables()
# TIMED ROWS 100000 IN SECS: insert_executemany 105.313201417
# TIMED ROWS 100000 IN SECS: just_inserts 105.22777814999999
# TIMED ROWS 100000 IN SECS: insert_execute_values 6.654337214000009
time_me('insert_executemany', insert_executemany)
time_me('just_inserts', just_inserts)
time_me('insert_execute_values', insert_execute_values)

