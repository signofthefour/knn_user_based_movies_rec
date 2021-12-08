import psycopg2
from tqdm import tqdm

database_config = {
    "host": "db-postgresql-movies-do-user-10242987-0.b.db.ondigitalocean.com",
    "user": "doadmin",
    "password": "oiYNLxLURdCUUaV9",
    "port": 25060,
    "database": "moviesrec"
}

db_connection = psycopg2.connect(**database_config)

lines = [(int(l.split(",")[0]), int(l.split(",")[1])) for l in open("user_based.csv", "r").readlines()]

cursor = db_connection.cursor()
# Print PostgreSQL details
print("PostgreSQL server information")
print(db_connection.get_dsn_parameters(), "\n")
# Executing a SQL query
cursor.execute("SELECT version();")
# Fetch result
record = cursor.fetchone()
print("You are connected to - ", record, "\n")


from tqdm import tqdm
explain = "Highly rated by most people like you"
db_connection.commit()

for pair in tqdm(lines[:100000]):
    # db_connection.commit()
    cursor.execute("INSERT INTO UserMovieRec VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", (int(pair[0]), int(pair[1]), 0, explain))

db_connection.commit() # Run after above process done
