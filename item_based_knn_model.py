# -*- coding: utf-8 -*-
"""k-NN_movie_lens.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1pnf5eC78MAvpoETweFlTG6XTjBIqMJ-n

# Connect to Database
"""

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

"""# Create Table"""

#     """ create tables in the PostgreSQL database"""
#     commands = (
#         """
#         CREATE TABLE KnnItemBasedRec (
#             vendor_id SERIAL PRIMARY KEY,
#             vendor_name VARCHAR(255) NOT NULL
#         )
#         """,
#         """ CREATE TABLE parts (
#                 part_id SERIAL PRIMARY KEY,
#                 part_name VARCHAR(255) NOT NULL
#                 )
#         """
#         )
#     conn = None
#     try:
#         # read the connection parameters
#         params = config()
#         # connect to the PostgreSQL server
#         conn = psycopg2.connect(**params)
#         cur = conn.cursor()
#         # create table one by one
#         for command in commands:
#             cur.execute(command)
#         # close communication with the PostgreSQL database server
#         cur.close()
#         # commit the changes
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()


# if __name__ == '__main__':
#     create_tables()

"""# Get data from table in database"""
print("Loading database...")
postgreSQL_select_Query ="""select * from \"Movies\""""
db_connection.commit()
cursor.execute(postgreSQL_select_Query)
movies_records = cursor.fetchall()

print('\tLoaded Movies table with: {} lines'.format(len(movies_records)))

# postgreSQL_select_Query ="""select * from UserMovieRec where model_id = 0"""
# db_connection.commit()
# cursor.execute(postgreSQL_select_Query)
# user_movie_rec_records = cursor.fetchall()
# print('\tLoaded UserMovieRec table with: {} lines'.format(len(user_movie_rec_records)))

postgreSQL_select_Query ="""select * from \"Ratings\""""
db_connection.commit()
cursor.execute(postgreSQL_select_Query)
ratings_records = cursor.fetchall()
print('\tLoaded Ratings table with: {} lines'.format(len(ratings_records)))

postgreSQL_select_Query ="""select * from \"Genres\""""
db_connection.commit()
cursor.execute(postgreSQL_select_Query)
genres_records = cursor.fetchall()
print('\tLoaded Genres table with: {} lines'.format(len(genres_records)))

postgreSQL_select_Query ="""select * from \"Users\""""
db_connection.commit()
cursor.execute(postgreSQL_select_Query)
users_records = cursor.fetchall()
print('\tLoaded Users table with: {} lines'.format(len(users_records)))

postgreSQL_select_Query ="""select * from \"MoviesGenres\""""
db_connection.commit()
cursor.execute(postgreSQL_select_Query)
movies_genres_records = cursor.fetchall()
print('\tLoaded MoviesGenres table with: {} lines'.format(len(movies_genres_records)))


if db_connection:
  cursor.close()
  db_connection.close()
  print("PostgreSQL connection is closed")

"""# Import Packages"""

import os
import time
import gc
import argparse
import numpy as np
# data science imports
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors
# utils import

"""# Load and format dataset

## Load users file
This dataset is stored in <df_users:pandas.DataFrame>
"""

# Read and format the data from the database
df_users = pd.DataFrame(users_records, columns=['UserID','Gender','Age','Occupation','Zip-code', 'x1', 'x2', 'x3'])
df_users['Gender'] = df_users['Gender'].apply(lambda x: 0 if x=='F' else 1)
df_users = df_users[['UserID','Gender','Age','Occupation','Zip-code']]
# Does we need Zip-code? -> maybe it help model to know the location (but the integer representation has no meaningm)
# Join or use current version of two tables? -> too large
# Information needed for recomen: User Side info ONLY? input (gender, age, occupation, [zipcode?], rating, [timestamp?])--> final:(user_id, movie_id)  
df_users.head()

"""## Load data from rating
This load dataset of rating into <df_ratings:pandas.DataFrame>
"""

df_ratings = pd.DataFrame(ratings_records, columns=['UserID','MovieID','Rating','Timestamp'])
df_ratings.head()

"""# Load data from movies
Load dataset of movies into <df_movies:pandas.DataFrame>
"""

movies = [movie[:3] for movie in movies_records]
print(movies[0])

df_movies = pd.DataFrame(movies, columns=['MovieID','Title','Year'])
df_movies.head()

df_genres = pd.DataFrame(genres_records, columns=["GenresID", "Name"])
genres = df_genres.to_dict()
genres_dict = {genres['GenresID'][id] : genres['Name'][id] for id in genres['GenresID']}
print(genres_dict)

df_movies_genres = pd.DataFrame(movies_genres_records, columns=['MovieID','Genres'])
df_movie_genres = df_movies_genres.groupby("MovieID").aggregate(lambda tdf: tdf.unique().tolist())
df_movie_genres.head()

df_movie_genres.loc[29].tolist()[0]

for index in range(len(df_movies)):
  movie_id = df_movies.loc[index]["MovieID"]
  genres = df_movie_genres.loc[movie_id].tolist()[0]
  genres_name = [genres_dict[genre] for genre in genres]

genres_list = list(genres_dict.values())
for genre in genres_list:
  df_movies[genre] = df_movies.apply(lambda x: False, axis=1)

for index in range(len(df_movies)):
  movie_id = df_movies.loc[index]["MovieID"]
  genres = df_movie_genres.loc[movie_id].tolist()[0]
  genres_name = [genres_dict[genre] for genre in genres]
  for genre_name in genres_name:
    df_movies.at[index, genre_name] = True


# df_movies = df_movies.drop(columns='Title')
df_movies.head()

"""## User_rating dataframe contain the information about who rate which movie"""

# Inner join two table

df_user_rating = pd.merge(df_users, df_ratings, 'inner', on=['UserID'])
# df_test = pd.DataFrame(df_user_rating.groupby('UserID'))
max_age = df_user_rating['Age'].max()
min_age = df_user_rating['Age'].min() # Fake Age should be considered, the people with fake age may have the same favor
max_occupation = df_user_rating['Occupation'].max()
min_occupation = df_user_rating['Occupation'].min()
print(f"{max_age}\t{min_age}\t{max_occupation}\t{min_occupation}")
# df_user_rating.drop(['Timestamp'])
df_user_rating.head()

"""# Combine and preprocess data"""

# movies preprocess
movieProperties = df_ratings.groupby('MovieID').agg({'Rating': [np.size, np.mean]})
movieProperties.head()

movieNumRatings = pd.DataFrame(movieProperties['Rating']['size'])
movieNormalizedNumRatings = movieNumRatings.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))


df_year = pd.DataFrame(df_movies[ ['MovieID', 'Year'] ])
max = df_year["Year"].max()
min = df_year["Year"].min()
print(max, min)
df_year["Year"] = df_year["Year"].apply(lambda x: (x - min) / (max - min))
df_year.head(30)
print(df_year.head(10))

df_movies[df_movies["Year"] == 1919]

movieDict = {}
movieIDs = [index for index, row in movieNormalizedNumRatings.iterrows()]
for index in range(len(df_movies)):
  movieID = df_movies.loc[index]["MovieID"]
  row = df_movies.loc[index]
  values = list(row.to_dict().values())
  feature = np.array([1 if v else 0 for v in values[1:-1]])
  if movieID not in movieIDs:
    continue
  size = movieNormalizedNumRatings.loc[movieID].get('size')
  mean_score = movieProperties.loc[movieID].Rating.get('mean')
  year = df_year.loc[df_year['MovieID'] == movieID]["Year"]
  movieDict[movieID] = (row["Title"], feature, \
                      size, \
                      mean_score,
                      float(year))

print(movieDict[29])

# transpose for dict
user_dict = df_users.T.to_dict()

print(user_dict[2])
user_dict = {user_dict[idx]["UserID"]: 
                (user_dict[idx]["UserID"],
                user_dict[idx]["Gender"], 
                float(user_dict[idx]["Occupation"] - min_occupation) / (max_occupation - min_occupation), 
                float(user_dict[idx]["Age"] - min_age) / (max_age - min_age), 
                user_dict[idx]["Zip-code"]) # str: just in case the same Zip-code, another is nonsense
                for idx in user_dict}
print(user_dict[2])

"""### Formular
- user based

$A = (age, gender, occupationm, zip-code)$

$B = (age, gender, occupationm, zip-code)$

$dist_{A,B} = |norm(A_{age}) - norm(B_{age})| + |norm(A_o) - norm(B_o)| + \mathbb{1}(A_{gender} - B_{gender}) + \frac{1}{2}\mathbb{1}(A_{zip-code} == B_{zip-code})$
"""

from scipy import spatial

def compute_dist(a, b, mode="item_based"):
    if mode=="item_based":
      genresA = a[1]
      genresB = b[1]
      genreDistance = spatial.distance.cosine(genresA, genresB) #range = [0,1]
      popularityA = a[2]
      popularityB = b[2]
      popularityDistance = abs(popularityA - popularityB) # range = [0,1]

      yearA = a[4]
      yearB = b[4]
      yearDistance = abs(yearA - yearB) # range[0,1]
      distances = [genreDistance, popularityDistance, yearDistance]
      
      return genreDistance + popularityDistance + yearDistance, distances
    if mode == "user_based":
      genderA = a[1]
      genderB = b[1]
      occupationA = a[2]
      occupationB = b[2]
      ageA, ageB = a[3], b[3]
      zipA, zipB = a[4], b[4]
      return abs(genderA - genderB) + abs(occupationA - occupationB) + abs(ageA - ageB) + 0.5*int(zipA == zipB)
print(len(movieDict))
print(movieDict[1], movieDict[2])
compute_dist(movieDict[1], movieDict[2], mode="item_based")

"""# Generate Rec tables"""

import operator

def getNeighbors(movieID, K):
    distances = []
    for movie in movieDict:
        if (movie != movieID):
            dist, dists = compute_dist(movieDict[movieID], movieDict[movie])
            distances.append((movie, dist, dists))

    distances.sort(key=operator.itemgetter(1))
    neighbors = []
    for x in range(K):
        neighbors.append([distances[x][0], distances[x][2]])
    return neighbors

def getSimilarUser(userID, K):
  distance = []
  for user in user_dict:
    if userID != user:
      dist = compute_dist(user_dict[userID], user_dict[user], "user_based")
      distance.append((user, dist))
  distance.sort(key=operator.itemgetter(1))
  neighbors = [dist[0] for dist in distance[0:K]]
  return neighbors

int(df_movies[df_movies["MovieID"] == 1]["Year"])

data = []
K = 10
avgRating = 0

fo = open('item_based.csv', 'w+')
from tqdm import tqdm
for item in tqdm(movieDict):
  # print(item)
  neighbors = getNeighbors(item, K)
  for neighbor, distances in neighbors:
      # avgRating += movieDict[neighbor][3]
      # print (movieDict[neighbor][0] + " " + str(movieDict[neighbor][3]))
      sorted_distances = [i[0] for i in sorted(enumerate(distances), key=lambda x: x[1])]
      genres_explain = "<br>Similar genres: " + ', '.join([genres_list[i] for i in range(len(genres_list)) if movieDict[neighbor][1][i] == 1 and movieDict[item][1][i] == 1]) + "</br>"
      year_explain = "<br>Close year: " + str(int(df_movies[df_movies["MovieID"] == neighbor]["Year"])) + "</br>"
      popularity_explain = '<br>Quite popular</br>' if distances[1] > 0.5 else '' 
      explains = [genres_explain, popularity_explain, year_explain]

      explain = '{}{}{}'.format(*[explains[i] for i in sorted_distances])
      fo.write(','.join([str(item), str(neighbor), explain + '\n']))