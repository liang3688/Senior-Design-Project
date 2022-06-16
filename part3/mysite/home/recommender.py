import pandas as pd
import numpy as np
import json
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class MoviesRecommender():
    def __init__(self):
        self.movies = None
        self.genre_table = None
    
    def read_data(self, filename):
        f = open(filename)
        data = json.load(f)
        self.movies = pd.json_normalize(data)
    
    def unique_genres(self):
        genres = list()
        for index, row in self.movies.iterrows():
            for i in row['info.genres']:
                 genres.append(i)
        return list(set(genres))
    
    def load_table(self):
        genres_str = self.movies['info.genres'].apply(lambda x : ' '.join(x))
        genres_str= genres_str.str.replace('Sci-Fi', 'SciFi')
        genres_str= genres_str.str.replace('Film-Noir','Noir')
        genres_str= genres_str.str.replace('Talk-Show','TalkShow')
        genres_str= genres_str.str.replace('Reality-TV','RealityTV')
        tfidf_vector = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf_vector.fit_transform(genres_str)
        self.genre_table = pd.DataFrame(data=tfidf_matrix.toarray(), 
                              index = self.movies['info.title'], 
                              columns = tfidf_vector.get_feature_names_out())
        return self.genre_table

    def similarity(self, movie):
        sim_movies = list()
        target_movie = np.array(self.genre_table.iloc[self.get_index_from_title(movie)]).reshape(1,-1)
        for i, row in self.genre_table.iterrows():
            sim_movies.append((i, cosine_similarity(target_movie, np.array(row).reshape(1,-1))[0][0]))
        return sim_movies  
    
    def get_index_from_title(self, title):
        return self.movies[self.movies['info.title'] == title].index.values[0]  
    
    def get_id_from_title(self, title):
        return self.movies[self.movies['info.title'] == title]['info.id'].values[0]        
    
    def get_title_from_index(self, index):
        return self.movies[self.movies.index == 0]['info.title'].values[0]
    
    def get_title_from_id(self, mid):
        try:
            return self.movies[self.movies['info.id'] == mid]['info.title'].values[0]
        except Exception as e:
            print(e)
            return None
        
    def get_img_from_title(self, title):
        return self.movies[self.movies['info.title'] == title]['info.image'].values[0]        
    
    def movies_recommend(self, movie, counts=10):
        sim_matrix = self.similarity(movie)
        sim_matrix.sort(key=lambda x: x[1], reverse=True)
        sim_matrix = list(filter(lambda x:x[0] != movie, sim_matrix))  # remove itself
        return [i[0] for i in sim_matrix[:counts]]

    
if __name__ ==  "__main__":
    mc = MoviesRecommender()
    mc.read_data('/home/ubuntu/data/movies.json')
    mc.load_table()
    print(mc.movies_recommend('Titanic'))
    print(mc.get_title_from_id(2857))