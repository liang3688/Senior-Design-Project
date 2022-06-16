import json
import csv
import os
import shutil
import sys
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Window
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.sql.functions import explode, col
import string

# path to the file to data folder
path = "/home/ubuntu/data/"

# uncomment one method for MASTER_URL 
MASTER_URL0 = "spark://localhost:7077"        # Spark standalone cluster
MASTER_URL1 = "local"                         # Locally with 1 thread
MASTER_URL2 = "local[2]"                      # Locally with 2 threads

methods = [MASTER_URL1, MASTER_URL2] 

for method in methods:
    
    # get the start time 
    start_time = time.time()

    spark = SparkSession.builder.master(method).appName("Read json").getOrCreate()

    # read the json file
    df = spark.read.option('header', 'true').json(path + "movies.json", multiLine = True)
  
    # select only the necessary columns. 
    df = df.select('info')
    df = df.select('info.starring', 'info.director', 'info.genres')
    #df.show(2)

    ### Get distinct actors from movies.json
    actors_list = df.select(explode(col("starring"))).distinct()
    #actors_list.show(2)

    ### Get distinct directors from movies.json
    directors_list = df.select(explode(col("director"))).distinct()
    #directors_list.show(2)

    ### Get distinct genres from movies.json
    genres_list = df.select(explode(col("genres"))).distinct()
    #genres_list.show()

    ### output to tsv files
    # remove actors_tsv folder if already exists
    if os.path.exists(path+ "actors_tsv"):
        shutil.rmtree(path + "actors_tsv")

    actors_list.write.format("csv").option("header", "true") \
            .option("delimiter", "\t").save(path + "actors_tsv")

    # remove director_tsv folder if already exists
    if os.path.exists(path+ "directors_tsv"):
        shutil.rmtree(path + "directors_tsv")
    
    directors_list.write.format("csv").option("header", "true") \
              .option("delimiter", "\t").save(path + "directors_tsv")
    
    # remove genres_tsv folder if already exists
    if os.path.exists(path+ "genres_tsv"):
        shutil.rmtree(path + "genres_tsv")
    
    genres_list.write.format("csv").option("header", "true") \
              .option("delimiter", "\t").save(path + "genres_tsv")
    
    # end time of the spark storing the data
    end_time = time.time()

    # output total time 
    print(f"Total time to create DB using {method} \t = {end_time - start_time} s")
    
### output the lists count   
print( "TOTAL NUMBER OF MOVIES    = ", df.count())
print( "TOTAL NUMBER OF ACTORS    = ", actors_list.count())
print( "TOTAL NUMBER OF DIRECTORS = ", directors_list.count())
print( "TOTAL NUMBER OF GENRES    = ", genres_list.count())   
print("Output the actors_tsv, directors_tsv and genres_tsv to users/maouc001/m_data")   
