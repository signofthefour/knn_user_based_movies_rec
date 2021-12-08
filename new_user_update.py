from scipy import spatial
import operator
from tqdm import tqdm
from collections import Counter
from database import get_cursor, get_records
import pandas as pd
import numpy as np

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
        return abs(genderA - genderB) + int(occupationA != occupationB) + abs(ageA - ageB) + 0.5*int(zipA != zipB)

def get_similar_user(new_user, user_dict, K):
    distance = []
    for user in user_dict:
        if user == new_user: continue
        dist = compute_dist(user_dict[new_user], user_dict[user], "user_based")
        distance.append((user, dist))
    distance.sort(key=operator.itemgetter(1))
    neighbors = [dist[0] for dist in distance[0:K]]
    return neighbors

def get_high_rated_by_user(df_ratings):
    df_rating_sorted = df_ratings.sort_values(['UserID','Rating'],ascending=False)[["UserID", "MovieID"]]

    high_rate_per_user_dict = {}

    for id in tqdm(range(len(df_rating_sorted))):
        user_id = df_rating_sorted.loc[id]['UserID']
        movie_id = df_rating_sorted.loc[id]['MovieID']
        if user_id not in high_rate_per_user_dict:
            high_rate_per_user_dict[user_id] = [movie_id]
        elif len(high_rate_per_user_dict[user_id]) > 20:
            continue
        else:
            high_rate_per_user_dict[user_id] += [movie_id]
    
    print(len(high_rate_per_user_dict))
    return high_rate_per_user_dict


def update_user(user_dict, high_rate_per_user_dict, user_id, K=10):

    neighbors = get_similar_user(user_id, user_dict, K)
    temp = []
    for neighbor in neighbors:
        temp.append((user_id, neighbor))

    fs = []
    for neighbor in [d[1] for d in temp]:
        fs += high_rate_per_user_dict[neighbor]
  
    rate = Counter(fs) # counts the elements' frequency
    sorted_rate = dict(sorted(rate.items(), key=lambda item: item[1])[::-1])
    res = [(user_id, movie_id) for movie_id in list(sorted_rate.keys())[:40]]

    return res 


def data_initialize( ):

    cursor, db_connection = get_cursor()
    print("Loading tables...")
    movies_records, ratings_records, genres_records, users_records, movies_genres_records = [get_records(cursor, db_connection, table_name) for table_name in ["Movies", "Ratings", "Genres", "Users", "MoviesGenres"]]    
    
    print("Load done\nPreprocessing data...")
    df_users = pd.DataFrame(users_records, columns=['UserID','Gender','Age','Occupation','Zip-code', 'x1', 'x2', 'x3'])
    df_users['Gender'] = df_users['Gender'].apply(lambda x: 0 if x=='F' else 1)
    df_users = df_users[['UserID','Gender','Age','Occupation','Zip-code']]

    df_ratings = pd.DataFrame(ratings_records, columns=['UserID','MovieID','Rating','Timestamp', ''])
    df_ratings = df_ratings[['UserID','MovieID','Rating','Timestamp']]

    movies = [movie[:3] for movie in movies_records]
    df_movies = pd.DataFrame(movies, columns=['MovieID','Title','Year'])
    df_genres = pd.DataFrame(genres_records, columns=["GenresID", "Name"])
    genres = df_genres.to_dict()
    genres_dict = {genres['GenresID'][id] : genres['Name'][id] for id in genres['GenresID']}

    df_movies_genres = pd.DataFrame(movies_genres_records, columns=['MovieID','Genres'])
    df_movie_genres = df_movies_genres.groupby("MovieID").aggregate(lambda tdf: tdf.unique().tolist())

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


    df_user_rating = pd.merge(df_users, df_ratings, 'inner', on=['UserID'])
# df_test = pd.DataFrame(df_user_rating.groupby('UserID'))
    max_age = df_user_rating['Age'].max()
    min_age = df_user_rating['Age'].min() # Fake Age should be considered, the people with fake age may have the same favor
    max_occupation = df_user_rating['Occupation'].max()
    min_occupation = df_user_rating['Occupation'].min()

    movieProperties = df_ratings.groupby('MovieID').agg({'Rating': [np.size, np.mean]})
    movieNumRatings = pd.DataFrame(movieProperties['Rating']['size'])
    movieNormalizedNumRatings = movieNumRatings.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))


    df_year = pd.DataFrame(df_movies[ ['MovieID', 'Year'] ])
    max = df_year["Year"].max()
    min = df_year["Year"].min()
    df_year["Year"] = df_year["Year"].apply(lambda x: (x - min) / (max - min))
    print("Preprocessing done")

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

    df_rating_sorted = df_ratings.sort_values(['UserID','Rating'],ascending=False)[["UserID", "MovieID"]]

    high_rate_per_user_dict = {}

    for id in tqdm(range(len(df_rating_sorted))):
        user_id = df_rating_sorted.loc[id]['UserID']
        movie_id = df_rating_sorted.loc[id]['MovieID']
        if user_id not in high_rate_per_user_dict:
            high_rate_per_user_dict[user_id] = [movie_id]
        elif len(high_rate_per_user_dict[user_id]) > 20:
            continue
        else:
            high_rate_per_user_dict[user_id] += [movie_id]

    #print(len(high_rate_per_user_dict))

    #age = (max_age-age)/(max_age-min_age)
    #occupation = (max_occupation - occupation)/(max_occupation-min_occupation)
    #gender = int(gender != 'F')
    #new_user = (user_id, gender, occupation, age, zip_code)
    return user_dict, high_rate_per_user_dict

def new_user(new_user_id, user_dict, high_rate_per_user_dict):
    print(user_dict[new_user_id])
    if user_dict == None:
        print("WRONG IN SOMETHING")
    res = update_user(user_dict, high_rate_per_user_dict, new_user_id)
    return res
