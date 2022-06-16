import json
import os
import sys
import time
import datetime
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql import Window
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from textblob import TextBlob
from pyspark.sql.types import FloatType
from pyspark.sql.functions import mean, min, max

# uncomment one method for MASTER_URL 
#MASTER_URL = "spark://localhost:7077"        # Spark standalone cluster
#MASTER_URL = "local"                         # Locally with 1 thread
MASTER_URL = "local[2]"                       # Locally with 2 threads

# path to the file to be clean 
PATH = "/home/ubuntu/data/"

# function which calculates sentiment polarity
def sentiment_analysis(review):
    return TextBlob(review).sentiment.polarity

# get the start time processing the cleaning
start_time = time.time()

spark = SparkSession.builder.master(MASTER_URL).appName("Read json").getOrCreate()

# read the json file
df = spark.read.option('header', 'true').json(PATH + "movie_reviews.json", multiLine = True)
print("")
#df.show(5)

# select only the necessary columns and create lower case review
df_clean = df.select( 'release_date', 'movie_name', 'review_type', \
           'reviewer_name', 'review_score', 'review', \
           (lower(regexp_replace('review', "[^a-zA-Z\\s]", "")).alias('review_lower')))

# tokenize the reviews
tokenizer = Tokenizer(inputCol = 'review_lower', outputCol = 'review_token')
df_review_token = tokenizer.transform(df_clean).select('release_date', \
            'movie_name', 'review_type', 'reviewer_name', 'review_score', \
            'review', 'review_token')

# remove stop words such as "the, she, he, ..."
remover = StopWordsRemover(inputCol = 'review_token', outputCol = 'review_clean')
df_review_no_stopw = remover.transform(df_review_token).select('release_date', \
                    'movie_name', 'review_type', 'reviewer_name', 'review_score', \
                    'review', 'review_token', 'review_clean')

# select only necessary columns. 
df_review_clean = df_review_no_stopw.select('release_date', 'movie_name', \
                    'review_type', 'reviewer_name', 'review_score', \
                    'review', 'review_clean')
#df_review_clean.show(5)

# remove commas between tokens in the review_clean column
df_review_clean = df_review_clean.withColumn("review_clean", \
                concat_ws(" ", col("review_clean")))

# create a sentiment_score column
s = df_review_clean.withColumn('sentiment_score', lit(0.0))

# for each review store its polarity in sentiment_score
s_analysis_udf = udf(sentiment_analysis , FloatType())
s = s.withColumn("sentiment_score", s_analysis_udf(s['review']))

# round the sentiment score to 3 decimals digits 
sent_df = s.withColumn("sentiment_score", round(col("sentiment_score"), 3))
print(f"\nShow cleaned reviews with sentiments")
sent_df.show(5)

# get the critic review only
critics_df = sent_df.filter(col("review_type") == "Critic Review") \
             .withColumn('review_score', col('review_score').cast('double'))                 
print(f"\nShow cleaned critic reviews with sentiments")
critics_df.show(5)

# get the user review only
users_df = sent_df.filter(col("review_type") == "User Review") \
             .withColumn('review_score', col('review_score').cast('double'))
print(f"\nShow cleaned user reviews with sentiments")
users_df.show(5)

# spark cleaning time
cleaning_time = time.time()

print('\n')
print(f"Total crawled reviews   = {df.count()}")
print(f"Total cleaned reviews   = {sent_df.count()}")
print(f"Total critic reviews    = {critics_df.count()}")
print(f"Total user reviews      = {users_df.count()}")

sentiment = sent_df.agg(fun.min(sent_df['sentiment_score']), \
                        fun.max(sent_df['sentiment_score']), \
                        fun.avg(sent_df['sentiment_score']))

critics   = critics_df.agg(fun.min(critics_df['review_score']), \
                        fun.max(critics_df['review_score']), \
                        fun.avg(critics_df['review_score']))

users     = users_df.agg(fun.min(users_df['review_score']), \
                        fun.max(users_df['review_score']), \
                        fun.avg(users_df['review_score']))

print('\n')
print(f"Min sentiment score     = {sentiment.collect()[0][0]}")
print(f"Max sentiment score     = {sentiment.collect()[0][1]}")
print(f"Average sentiment score = {sentiment.collect()[0][2]}")

print('\n')
print(f"Min critics score       = {critics.collect()[0][0]}")
print(f"Max critics score       = {critics.collect()[0][1]}")
print(f"Average critics score   = {critics.collect()[0][2]}")

print('\n')
print(f"Min  user score         = {users.collect()[0][0]}")
print(f"Max  user score         = {users.collect()[0][1]}")
print(f"Average  user score     = {users.collect()[0][2]}")

#total time for park processing
end_time = time.time()

print('\n')
print(f"SPARK using 2 threads:")
print('\n')
print(f"\tSpark data cleaning time        = {cleaning_time - start_time} seconds")   
print(f"\tSpark calculation time          = {end_time - cleaning_time} seconds")  
print(f"\tTotal time for spark processing = {end_time - start_time} seconds")
print('\n')

# remove folder if already exists
if os.path.exists(PATH + "cleaned_reviews"):
    shutil.rmtree(PATH + "cleaned_reviews")

print("Uncomment one option to save file as csv or json file")

#### output to a csv file
sent_df.write.csv(path= PATH + 'cleaned_reviews', header=True, sep='\t')

#### output to a json file
#sent_df.write.json(PATH + 'cleaned_reviews')

print("Output is in PATH/cleaned_reviews folder")

# Stop Spark session
spark.stop()
    

