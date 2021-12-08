from tqdm import tqdm

database_config = {
    "host": "db-postgresql-movies-do-user-10242987-0.b.db.ondigitalocean.com",
    "user": "doadmin",
    "password": "oiYNLxLURdCUUaV9",
    "port": 25060,
    "database": "moviesrec"
}

import psycopg2

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

lines = [l.split(",") for l in open("item_based.csv", "r").readlines()]

def update_item_based_model(option="all"):
    if option == "all":
        for pair in tqdm(lines):
            db_connection.commit()
            cursor.execute("INSERT INTO MovieRec(movie_id, rec_id, explain) VALUES (%s,%s,%s) ON CONFLICT (movie_id, rec_id) DO UPDATE SET explain=%s", \
                (int(pair[0]), int(pair[1]), pair[2], pair[2]))
    else:
        pass

update_item_based_model(option="all")