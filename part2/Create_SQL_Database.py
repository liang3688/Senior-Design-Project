# source your_env/bin/activate
# cd users/maouc001 > python3 Create_SQL_Database.py

import csv
import datetime
import json
import os
import time

import mysql.connector
from mysql.connector.cursor import MySQLCursor

## File's paths:   (make sure they are updated in the project data folder) 
CONFIG_PATH       = "/home/ubuntu/final_submit/part2/config.json"
MOVIES_JSON_PATH  = "/home/ubuntu/data/movies.json"                          # the data folder in in the google drive linked in report
REVIEWS_TSV_PATH = "/home/ubuntu/data/cleaned_reviews.tsv"                   # the data folder in in the google drive linked in report
#REVIEWS_JSON_PATH = "/home/ubuntu/users/maouc001/m_data/cleaned_reviews.json"

# Read MySQL username, password, schema from configuration file
with open(CONFIG_PATH, "r") as f:
    jobj        = json.load(f)
    MYSQL_USER  = jobj["mysql_user"]
    MYSQL_PWD   = jobj["mysql_pwd" ]
    MYSQL_DB    = jobj["mysql_db"  ]
    #BATCH_SIZE  = int(jobj.get("batch_size", 1))
    #INDEX_AFTER = jobj.get("index_after_load", False)

assert MYSQL_USER
assert MYSQL_PWD
assert MYSQL_DB

# Open connection to MySQL
conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PWD,
                               host="localhost", database=MYSQL_DB)

# Enter tables to database
cursor = conn.cursor()

## clean the database tables if they exist
cursor.execute("DROP TABLE IF EXISTS critique;"      )
cursor.execute("DROP TABLE IF EXISTS reviews;"       )
cursor.execute("DROP TABLE IF EXISTS movie_genre;"   )
cursor.execute("DROP TABLE IF EXISTS plays_in;"      )
cursor.execute("DROP TABLE IF EXISTS directed;"      )
cursor.execute("DROP TABLE IF EXISTS critic_reviews;")
cursor.execute("DROP TABLE IF EXISTS user_reviews;"  )
cursor.execute("DROP TABLE IF EXISTS stream_links;"  ) 
cursor.execute("DROP TABLE IF EXISTS genres;"        )
cursor.execute("DROP TABLE IF EXISTS actors;"        )
cursor.execute("DROP TABLE IF EXISTS directors;"     )
cursor.execute("DROP TABLE IF EXISTS movies;"        )

## 1 Create/Recreate movies table
cursor.execute(
    "CREATE TABLE movies ("
    "movie_id INT PRIMARY KEY,"                      # 1- movie ID from json file 
    "title TEXT,"                                    # 2- movie title
    "release_date DATE,"                             # 3- release Date
    "distributor VARCHAR(100),"                      # 4- distributor
    "ratings VARCHAR(16),"                           # 5- movie ratings (ex: PG13, R, ...)
    "runtime INT,"                                   # 6- movie runtime in minutes
    "summary TEXT,"                                  # 7- summary of the movie
    "m_u_score DECIMAL(3,1),"                        # 8- avg of all user given scores 
    "m_c_score DECIMAL(3,1),"                        # 9- avg of critic given scores  
    "image VARCHAR(256)"                             #10- image of the movie cover
    ");")

## 2 Create/Recreate directors table
cursor.execute(
    "CREATE TABLE directors ("
    "director_id INT AUTO_INCREMENT PRIMARY KEY,"    # 1- director ID (AUTO)
    "name VARCHAR(64)"                               # 2- name
    ");")

## 3 Create/Recreate actors table
cursor.execute(
    "CREATE TABLE actors ("
    "actor_id INT AUTO_INCREMENT PRIMARY KEY,"       # 1- actor ID (AUTO)        
    "name VARCHAR(64)"                               # 2- name
    ");")

## 4 Create/Recreate genres table
cursor.execute(
    "CREATE TABLE genres ("
    "genre_id INT AUTO_INCREMENT PRIMARY KEY,"       # 1- genre ID (AUTO)
    "genre_type VARCHAR(32)"                         # 2- genre Type
    ");")

## 5 Create/Recreate stream_links & watch_in table (weak entity)
cursor.execute(
    "CREATE TABLE stream_links ("
    "movie_id INT,"                                  # 1- movie ID
    "url VARCHAR(512),"                              # 2- url to the movie
    "PRIMARY KEY (movie_id, url),"           
    "FOREIGN KEY (movie_id) REFERENCES movies(movie_id)" 
    ");")

## 6 Create/Recreate user_reviews table 
cursor.execute(
    "CREATE TABLE user_reviews ("
    "review_id INT AUTO_INCREMENT PRIMARY KEY,"      # 1- user review id
    "u_c_review TEXT,"                               # 2- clean review
    "u_review TEXT,"                                 # 3- review given by the user
    "username VARCHAR(64),"                          # 4- username who gave the review
    "u_score DECIMAL(3,1),"                          # 5- score given by user (10.0)
    "polarity DECIMAL(4,3)"                          # 6- polarity   [-1.0, +1.0]
    ");")

## 7 Create/Recreate critic_reviews table 
cursor.execute(
    "CREATE TABLE critic_reviews ("
    "critic_id INT AUTO_INCREMENT PRIMARY KEY,"      # 1- critic id
    "c_c_review TEXT,"                               # 2- clean review
    "c_review TEXT,"                                 # 3- review given by the critic or user
    "c_score DECIMAL(3,1),"                          # 4- critic given score 10.0
    "polarity DECIMAL(4,3)"                          # 5- polarity
    ");")

## HANDLE RELATIONSHIP TABLES ##  

## 8 Create/Recreate directed table   
cursor.execute(
    "CREATE TABLE directed ("                        # relation b/w movies & directors      
    "movie_id INT,"                                      
    "director_id INT,"   
    "PRIMARY KEY (movie_id, director_id),"
    "FOREIGN KEY (movie_id) REFERENCES movies(movie_id),"      
    "FOREIGN KEY (director_id) REFERENCES directors(director_id)"                     
    ");")

## 9 Create/Recreate plays_in table
cursor.execute(
    "CREATE TABLE plays_in ("                       # relation b/w movies & actors           
    "movie_id INT,"                                      
    "actor_id INT,"  
    "PRIMARY KEY (movie_id, actor_id)," 
    "FOREIGN KEY (movie_id) REFERENCES movies(movie_id),"      
    "FOREIGN KEY (actor_id) REFERENCES actors(actor_id)"                     
    ");")

## 10 Create/Recreate movie_genre table
cursor.execute(
    "CREATE TABLE movie_genre ("                    # relation b/w movies & their genre       
    "movie_id INT,"                                      
    "genre_id INT,"   
    "PRIMARY KEY (movie_id, genre_id),"
    "FOREIGN KEY (movie_id) REFERENCES movies(movie_id),"      
    "FOREIGN KEY (genre_id) REFERENCES genres(genre_id)"                     
    ");")

## 11 Create/Recreate reviews table
cursor.execute(
    "CREATE TABLE reviews ("                       # relation b/w movies & their user_review
    "movie_id INT,"                                   
    "review_id INT,"                                
    "PRIMARY KEY (movie_id, review_id),"           
    "FOREIGN KEY (movie_id) REFERENCES movies(movie_id)," 
    "FOREIGN KEY (review_id) REFERENCES user_reviews(review_id)" 
    ");")

## 12 Create/Recreate critique table
cursor.execute(
    "CREATE TABLE critique ("                      # relation b/w movies & their critic_review
    "movie_id INT,"                                   
    "critic_id INT,"                                
    "PRIMARY KEY (movie_id, critic_id),"           
    "FOREIGN KEY (movie_id) REFERENCES movies(movie_id)," 
    "FOREIGN KEY (critic_id) REFERENCES critic_reviews(critic_id)" 
    ");")

# table insert statements         
insert_to_movies         = "INSERT INTO movies(movie_id, title, release_date, distributor," \
                           "ratings, runtime, summary, m_u_score, m_c_score, image) " \
                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

insert_to_directors      = "INSERT INTO directors (name) Values (%s)"
insert_to_directed       = "INSERT INTO directed (movie_id, director_id) Values (%s,%s)"
    
insert_to_actors         = "INSERT INTO actors (name) Values (%s)"
insert_to_plays_in       = "INSERT INTO plays_in (movie_id, actor_id) Values (%s,%s)"

insert_to_genres         = "INSERT INTO genres (genre_type) Values (%s)"
insert_to_movie_genre    = "INSERT INTO movie_genre (movie_id, genre_id) Values (%s,%s)"
    
insert_to_stream_links   = "INSERT INTO stream_links (movie_id, url) Values (%s,%s)"
    
insert_to_user_reviews   = "INSERT INTO user_reviews (u_c_review, u_review," \
                           "username, u_score, polarity) Values (%s,%s,%s,%s,%s)"
insert_to_reviews        = "INSERT INTO reviews (movie_id, review_id) Values (%s,%s)"

insert_to_critic_reviews = "INSERT INTO critic_reviews (c_c_review, c_review," \
                           "c_score, polarity) Values (%s,%s,%s,%s)"
insert_to_critique       = "INSERT INTO critique (movie_id, critic_id) Values (%s,%s)"

# arrays which are used to keep track of data already entered
movie_list    = []  # saves array element as [release_date, title, m_id] 
director_list = []  
director_ids  = []
actor_list    = []  
actor_ids     = []
genre_list    = []
genre_ids     = []  
   
# hold id's for the tables. 
movie_count = 0
director_id = 0
actor_id    = 0
genre_id    = 0 
review_id   = 0
critic_id   = 0
review_count= 0
movies_without_date = 0

# get the start time 
start_time = time.time()

# go through the movie.json file
with open(MOVIES_JSON_PATH) as f:
    data = json.load(f)
            
    for row in data:
        m_id    = int(row['info']['id'])            
        title   = row['info']['title']        
        dist    = row['info']['distributor']          
        rating  = row['info']['rating'] 
        summary = row['info']['summary'] 
        c_score = float(row['meta_score']['score'])/10      
        image   = row['info']['image']
        
        try:
            date    = str(datetime.datetime.strptime(row['info']['date'], \
                                        '%B %d, %Y').strftime('%Y-%m-%d'))
        except ValueError:
            date = None 
            movies_without_date = movies_without_date + 1
                      
        try:
            runtime = int(row['info']['runtime'].split()[0])
        except AttributeError:
            runtime = None     
        
        try:
            u_score = float(row['user_score']['score'])
        except ValueError:
            u_score = None 
         
        movie_count = movie_count+1
        movie_list.append([date, title, m_id])   
        cursor.execute(insert_to_movies, (m_id, title, date, dist, \
                       rating, runtime, summary, u_score, c_score, image))
        
        url_data= row['info']['stream_link'] 
        for url in url_data:
            cursor.execute(insert_to_stream_links, (m_id, url))
        
        director_data = list(set(row['info']['director']))
        for director in director_data:
            try:
                d_index = director_list.index(director)                
                cursor.execute(insert_to_directed, (m_id, director_ids[d_index]))     
            except ValueError:                
                director_id = director_id + 1
                director_list.append(director)
                director_ids.append(director_id)                
                cursor.execute(insert_to_directors, (director,))
                cursor.execute(insert_to_directed, (m_id, director_id))
                
        actor_data = list(set(row['info']['starring'])) 
        for actor in actor_data:
            try:
                a_index = actor_list.index(actor)
                cursor.execute(insert_to_plays_in, (m_id, actor_ids[a_index]))
            except:
                actor_id = actor_id + 1
                actor_list.append(actor)
                actor_ids.append(actor_id)
                cursor.execute(insert_to_actors, (actor,))
                cursor.execute(insert_to_plays_in, (m_id, actor_id))
                         
        genre_data = list(set(row['info']['genres'])) 
        for genre in genre_data:
            try:
                g_index = genre_list.index(genre)
                cursor.execute(insert_to_movie_genre, (m_id, genre_ids[g_index]))
            except:
                genre_id = genre_id + 1
                genre_list.append(genre)
                genre_ids.append(genre_id)
                cursor.execute(insert_to_genres, (genre,))    
                cursor.execute(insert_to_movie_genre, (m_id, genre_id))
                               
    # go through the reviews.tsv file
    with open(REVIEWS_TSV_PATH, "r") as f:
        csv_rdr = csv.DictReader(f, delimiter="\t")
                
        for row in csv_rdr:

#     # go through the movie.json file
#     with open(REVIEWS_JSON_PATH) as f:
#         reviews_file = json.load(f)
        
#         for row in reviews_file: 
            title        = row['movie_name']        
            review_type  = row['review_type'] 
            review       = row['review']
            clean_review = row['review_clean']
            
            try:
                polarity = float(row['sentiment_score'])
            except TypeError:
                polarity = None
            except ValueError:
                polarity = None          
            
            try:
                review_score = float(row['review_score'])
            except TypeError:
                review_score = None
                    
            try:
                date    = str(datetime.datetime.strptime(row['release_date'], \
                                            '%B %d, %Y').strftime('%Y-%m-%d'))   
            except ValueError:
                date = None 
                        
            review_count = review_count + 1
        
            for movie in movie_list:
                if(date == movie[0]) and (title == movie[1]):
                    movie_id = movie[2]
                    break
                if((date == "TBA") or (date == None)) and (title == movie[1]):
                    movie_id = movie[2]
                    break                
        
            if (review_type == "User Review"):
                username  = row['reviewer_name']            
                review_id = review_id+1
            
                cursor.execute(insert_to_user_reviews, (clean_review, review, username, \
                                                        review_score, polarity))
                cursor.execute(insert_to_reviews, (movie_id, review_id))
                
            if (review_type == "Critic Review"):
                critic_id    = critic_id+1 
                review_score = review_score / 10     # convert 100 to 10 point scale
                #polarity     = TextBlob(review).sentiment.polarity
                
                cursor.execute(insert_to_critic_reviews, (clean_review, review, \
                                                          review_score, polarity))
                cursor.execute(insert_to_critique, (movie_id, critic_id))    
            
conn.commit()
    
# end time of the spark storing the data
end_time = time.time()
print(f"Total time to create MySQL DB \t= {end_time - start_time} s")

# print some values
print(f"Number of actors         = {actor_id}    "                )
print(f"Number of directors      = {director_id} "                )
print(f"Number of genres         = {genre_id}    "                )
print(f"Number of user reviews   = {review_id}   "                )
print(f"Number of critic reviews = {critic_id}   "                )
print(f"Number of total reviews  = {review_count}"                )
print(f"Number of movies         = {movie_count} "                )
print(f"Number of movies w/o release date = {movies_without_date}")

## close conn (use db_test notebook to test if db is downloaded properly) 
cursor.close()
conn.close()