import os
import time
import numpy as np
import pandas as pd
import psycopg2

def get_cursor():
    database_config = {
        "host": "db-postgresql-movies-do-user-10242987-0.b.db.ondigitalocean.com",
        "user": "doadmin",
        "password": "oiYNLxLURdCUUaV9",
        "port": 25060,
        "database": "moviesrec"
    }

    db_connection = psycopg2.connect(**database_config)
    db_connection.commit()

    cursor = db_connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(db_connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
    return cursor, db_connection


def get_records(cursor, db_connection, table_name=""):
    if table_name not in ["Movies", "Ratings", "Genres", "Users", "MoviesGenres"]:
        return None
    postgreSQL_select_Query ="select * from \"{}\"".format(table_name)
    db_connection.commit()
    cursor.execute(postgreSQL_select_Query)
    records = cursor.fetchall()
    return records


