import psycopg2
from datetime import datetime
from typing import NamedTuple
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values


def _get_connection(host, dbname, user, password):
    dsn = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(host, dbname, user, password)
    print("Connecting to database [" + dsn + "]")
    return psycopg2.connect(dsn)


def get_connection():
    return _get_connection(host='localhost', dbname='movies', user='postgres', password='postgres')


def create_database():
    print('Creating database')
    conn = psycopg2.connect("host='localhost' user='postgres' password='postgres'")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS movies;")
    cur.execute("CREATE DATABASE movies;")
    cur.close()


def create_movie_table(conn):
    print('Creating table movie')
    cur = conn.cursor()
    cur.execute(
        """
    CREATE TABLE movie (
        id          SERIAL PRIMARY KEY,
        year        INT,
        title       TEXT,
        rating      NUMERIC(3,1),
        plot        TEXT,
        modified    TIMESTAMP
    );
    """
    )
    conn.commit()
    cur.close()


def insert_executemany(conn):
    # Insert batch using executemany
    # TIMED ROWS 100000 IN SECS: insert_executemany 105.313201417
    modified = datetime.now()
    data = [
        (2016, 'Hacksaw Ridge', 8.1, 'WWII Bad ass medic', modified),
        (2016, 'Arrival', 7.9, 'Speaking with aliens', modified),
        (2016, 'Captain America: Civil War ', 7.8, 'Captain America does not compromise', modified),
        (2016, 'Rogue One ', 7.8, 'Rebel Alliance kicks some imperial butt', modified),
        (2016, 'Deadpool', 8, 'Dirty jokes and dead people', modified),
    ]
    cur = conn.cursor()
    sql = "insert into movie (year, title, rating, plot, modified) values  (%s, %s, %s, %s, %s)"
    cur.executemany(sql, data)
    conn.commit()
    cur.close()


def insert_loop(conn):
    # Insert batch using sequence of inserts
    # How to get id of a newly created record?
    # TIMED ROWS 100000 IN SECS: just_inserts 105.22777814999999
    modified = datetime.now()
    data = [
        (2020, 'Ready Player One', 6.1, 'Bad girls kill bad guys', modified),
        (2020, 'The Call of the Wild', 6.8, 'A sled dog struggles for survival in the wilds of the Yukon.', modified),
        (2019, 'Joker', 8.5, '', modified),
        (2019, 'Avenger: Endgame', 8.4, '', modified),
        (2019, 'Spider-Man: Far from Home', 7.5, '', modified),
        (2018, 'Ready Player One', 7.5, 'Easter Egg hunt in Matrix', modified)
    ]

    sql = "insert into movie (year, title, rating, plot, modified) values (%s, %s, %s, %s, %s)"
    cur = conn.cursor()
    for d in data:
        cur.execute(sql, d)
    conn.commit()
    cur.close()


def insert_execute_values(conn):
    # Insert batch using psycopg2.extras.execute_values
    # TIMED ROWS 100000 IN SECS: insert_execute_values 6.654337214000009
    modified = datetime.now()
    data = [
        (2017, 'Blade Runner 2049', 8, 'Searching for Harrison Ford', modified),
        (2017, 'Twin Peaks', 8.5, '25 years after', modified),
        (2017, 'Dunkirk', 7.9, 'Evacuate us!!', modified),
        (2017, 'War for the Planet of the Apes', 7.4, 'Take that humans!', modified),
        (2017, 'Wonder Woman', 7.4, 'Ares gets a beat down from a girl', modified),
    ]
    cur = conn.cursor()
    sql = "insert into movie (year, title, rating, plot, modified) values %s"
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()




class Movie(NamedTuple):
    id: int
    year: str
    title: str
    rating: float
    plot: str
    modified: datetime


def to_movie(row):
    return Movie(
        id=row[0],
        year=row[1],
        title=row[2],
        rating=row[3],
        plot=row[4],
        modified=row[5],
    )


def fetch_batches(batch_size, fetch_since, process_batch):
    conn = get_connection()
    try:
        with conn.cursor(name='my-cursor') as cur:
            total_rows = 0
            cur.execute("select id, year, title, rating, plot, modified from movie where modified >= %s order by id", [fetch_since])
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                batch = []
                for row in rows:
                    emp = to_movie(row)
                    batch.append(emp)
                    total_rows += 1
                print('Batch processed')
                process_batch(batch)

            print("Total movies fetched", total_rows)
    finally:
        conn.close()


def fetch_all():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("select id, year, title, rating, plot, modified from movie order by year, title")
            rows = cur.fetchall()
            return [to_movie(row) for row in rows]  # Fetchall returns a tuple for each row. Map rows to movies.
    finally:
        conn.close()


def get_last_fetch_time():
    return datetime.fromisoformat('2010-01-01T00:00:00')


def print_movies(movies):
    for m in movies:
        print(str(m.year) + ' ' + m.title + ' (imdb ' + str(m.rating) + ') ' + m.plot)


if __name__ == "__main__":
    create_database()
    db = get_connection()
    create_movie_table(db)
    insert_loop(db)
    insert_execute_values(db)
    insert_executemany(db)
    db.close()
    fetch_batches(5, get_last_fetch_time(), print_movies)
    print('ALL MOVIES')
    print_movies(fetch_all())
    
