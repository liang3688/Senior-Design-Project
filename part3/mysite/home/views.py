import json
import os
import re
import mysql.connector
from . import recommender
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

ROOT = os.path.dirname(os.path.dirname(
       os.path.dirname(os.path.realpath(__file__))))

# Movie to recommend based on the selected genres
mc = recommender.MoviesRecommender()
mc.read_data('/home/ubuntu/data/movies.json')

# File path
CONFIG_PATH = os.path.join("/home/ubuntu/mysite/", "config.json")

# Read MySQL username, password, database from configuration file
with open(CONFIG_PATH, "r") as f:
    jobj       = json.load(f)
    MYSQL_USER = jobj["mysql_user"]
    MYSQL_PWD  = jobj["mysql_pwd" ]
    MYSQL_DB   = jobj["mysql_db"  ]
    
@csrf_exempt
def index(request):
    return render(request, "index.html")

# function to handle advance search(not implemented)
    # @csrf_exempt
    # def advanced(request):
    #     return render(request, "advanced.html")


# function to display user reviews
    # @csrf_protect
    # def display_user_reviews(request):
    #     return render(request, "user_reviews.html")

# function to display critic reviews
    # @csrf_protect
    # def display_critic_reviews(request):
    #     return render(request, "critic_reviews.html")

# function to handle the search request
#@csrf_exempt
#@csrf_protect
def search(request):
    print(request.method)
    if request.method != "POST":
        return HttpResponse("Invalid request!")
    
    params = request.POST

    values = []
    
    search_by = params.get("filter", "title")
    
    query = f"SELECT DISTINCT m.movie_id, m.image, m.title, m.release_date, " \
            f"m.distributor, m.ratings, m.runtime, m.m_c_score, " \
            f"m.m_u_score, m.summary "
    
    if search_by == "actor":
        query += f"FROM actors a, movies m, plays_in p " \
                 f"WHERE m.movie_id = p.movie_id " \
                 f"AND a.actor_id = p.actor_id " \
                 f"AND a.name "
    
    elif search_by == "director":
        query += f"FROM directors d, movies m, directed dir "\
                 f"WHERE m.movie_id = dir.movie_id " \
                 f"AND d.director_id = dir.director_id " \
                 f"AND d.name "
    
    elif search_by == "genre":
        query += f"FROM movies m, movie_genre mg, genres g "\
                 f"WHERE m.movie_id = mg.movie_id " \
                 f"AND mg.genre_id = g.genre_id " \
                 f"AND g.genre_type " 
        
    elif search_by == "distributor":
        query += f"FROM movies m " \
                 f"WHERE m.distributor "
        
    else:
        query += f"FROM movies m " \
                 f"WHERE m.title "
    
    regex = "[a-zA-Z0-9]+[^a-zA-Z0-9]*[a-zA-Z0-9]+"
    #p = re.compile(regex)
    
    if params.get("keyword", ""):
        #values.append("%s")
        if(re.search(regex, params["keyword"])):
            values.append(params["keyword"].lower())
        else:
            return HttpResponse("Please enter a valid keyword!!")
    else:
        return HttpResponse("Please enter a keyword!!")
    
    # complete the query
    query += f"LIKE \'%" + "".join(values) + "%\' "
    
    order_by = params.get("OrderBy_filter", "release_date")

    if(order_by == "metacritic_score"):
        query += f"ORDER BY m.m_c_score DESC;" 

    elif(order_by == "user_score"):
        query += f"ORDER BY m.m_u_score DESC;" 

    else :      # default order by DESC release date
        query += f"ORDER BY m.release_date DESC;" 
    
    print(query)
    
    # Open connection to MySQL
    conn = mysql.connector.connect(user=MYSQL_USER, 
           password=MYSQL_PWD, host="localhost", database=MYSQL_DB)
    cursor = conn.cursor()
    
    # Execute the SELECT query
    cursor.execute(query)

    lines = [
        "<!DOCTYPE html>",
        "<html lang=\"en\">",
        "<head>",
        "<style>",
        "table, th, td {",
        "  border:1px solid black;",
        "}",
        "body {",
        "  background-image:url(\"/static/img/bg4.jpg\");",
        "  background-repeat:no-repeat;",
        "  background-size:cover;",
        "}",
        "</style>",
        "</head>",
        "<body>",
        "<a href=/home/><button style=\"height:40px;width:60px\">Home</button></a><br></br>",
        "<table>",
        "  <tr>",
        "    <th>#</th>",
        #"    <th>ID</th>",  display for test only
        "    <th>Image</th>",
        "    <th>Movie Name</th>",
        "    <th>Release Date</th>",
        "    <th>Director</th>",
        "    <th>Cast</th>",
        "    <th>Distributor</th>",
        "    <th>Genre</th>",
        "    <th>Rating</th>",
        "    <th>Run time (Minutes)</th>",
        "    <th>Metascore</th>",
        "    <th>User Score</th>", 
        "    <th>Summary</th>",
        "  </tr>",
    ]
    
    ## fetch culumns = [0] movie_id     [1] image    [2] title    [3] release_date
    #                  [4] distributor  [5] ratings  [6] runtime  [7] critic_score
    #                  [8] user_score   [9] movie summary       
    
    num = 1
    fetch = cursor.fetchall()
    lines.append(f"<div style=font-size:20px;>{len(fetch)} Results Found</div><br>")
    for row in fetch:        
        lines.append("  <tr>")
        # Result number
        lines.append(f"    <th>{num}</th>")
        num += 1

        lines.append(f"    <th><img src=\"{row[1]}\" width=130 height=200></th>")
        
        lines.append(f"    <th> "
                     f"      <form action=\"more\" method=\"post\"> "
                     f"        <div>{row[2]}</div> "
                     f"        <div>"
                     f"          <input style=\"height:30px;width:120px\" type=\"submit\" value=\"Show more info\">"
                     f"        </div>"
                     f"        <input type=\"hidden\" name =\"data\" value=\"{row[0]}\">"
                     f"      </form>"
                     f"    </th>"
                    )
        
        if row[3] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[3]}</th>")
        
        lines.append(f"    <th>")
        
        query_director = f"SELECT d.name " \
                         f"FROM directors d, directed dd " \
                         f"WHERE d.director_id = dd.director_id " \
                         f"AND dd.movie_id = " + str(row[0]) + " " \
                         f"ORDER BY d.name ASC;"
        
        cursor.execute(query_director)
        fetch_directors = cursor.fetchall()

        if len(fetch_directors):
            for director in fetch_directors:
                lines.append(f"<span>{director[0]}</span>")
                lines.append(f",")
            lines[-1:] = ""
        else:
            lines.append(f"<div>N/A</div>") 
        
        lines.append(f"</th>")
        
        lines.append(f"    <th><span>")
        
        query_actor = f"SELECT a.name FROM actors a, plays_in p " \
                      f"WHERE a.actor_id = p.actor_id " \
                      f"AND p.movie_id = " + str(row[0]) + " " \
                      f"ORDER BY a.name ASC;"
        
        cursor.execute(query_actor)
        
        fetch_actors = cursor.fetchall()
        # if fetch_actors:
        #for actors in fetch_actors:

        if len(fetch_actors):
            for actor in fetch_actors:
                lines.append(f"<span>{actor[0]}</span>")
                lines.append(f",")
            lines[-1:] = ""
        else:
            lines.append(f"<div>N/A</div>") 
        
        lines.append(f"</span></th>")
        
        if row[4] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[4]}</th>")
        
        lines.append(f"</th>")
        
        lines.append(f"    <th>")
        
        query_genre = f"SELECT g.genre_type, g.genre_id " \
                      f"FROM genres g, movie_genre mg " \
                      f"WHERE g.genre_id = mg.genre_id " \
                      f"AND mg.movie_id = {row[0]} " \
                      f" ORDER BY g.genre_type ASC;"
        
        cursor.execute(query_genre)
        fetch_genres = cursor.fetchall()
        
        if len(fetch_genres):
            for genre in fetch_genres:
                lines.append(f"<span>{genre[0]}</span>")
        else:
            lines.append(f"<span>N/A</span>") 
        
        if row[5] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[5]}</th>")
            
        if row[6] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[6]}</th>")
        
        if row[7] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[7]}</th>")
        
        if row[8] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[8]}</th>")
                
        if row[9] is None:
            lines.append(f"    <th>N/A</th>")
        else:
            lines.append(f"    <th>{row[9]}</th>")
                
        lines.append("  </tr>")
    lines.append("</table>")
    lines.append("</body>")
    lines.append("</html>")
    cursor.close()
    conn.close()
    return HttpResponse("\n".join(lines))

# function which displays the selected movie from the search
@csrf_exempt
def more(request):
    print(request.method)
    if request.method != "POST":
        return HttpResponse("Invalid request!")
    
    params = request.POST

    movie_id = request.POST['data']
    
    lines = [
        "<!DOCTYPE html>",
        "<html lang=\"en\">",
        "<head>",
        '<link rel="stylesheet" href="/static/css/format.css">',
        "<style>",
        "body {",
        "  background-image:url(\"/static/img/bg11.jpg\");",
        "  background-repeat:no-repeat;",
        "  background-size:cover;",
        "}",
        "</style>",
        "<body>",
        "<a href=/home/><button style=\"height:40px;width:60px\">Home</button></a><br></br>",
    ]
    
    conn = mysql.connector.connect(user=MYSQL_USER, 
           password=MYSQL_PWD, host="localhost", database=MYSQL_DB)
    cursor = conn.cursor()
    
    query = f"SELECT * FROM movies WHERE movie_id = {movie_id};"
    cursor.execute(query)
    fetch = cursor.fetchall()
    
    # load similarity matrix
    mc.load_table()
    sim_movies = mc.movies_recommend(mc.get_title_from_id(int(movie_id)))
    
    lines.append(f"<div style=\"width: 100%;\">")
    for row in fetch:
    
        lines.append(f"     <div style=\"width: 15%; float:left;\"><img src=\"{row[9]}\" width=220 height=300></div>")
        lines.append(f"     <div style=\"margin-left: 16%;\">")
        lines.append(f"          <div><b style=\"font-size:25px\">{row[1]}</b></div>")
        lines.append(f"          <div><b style=\"font-size:18px\">Release Date: </b><span>{row[2]}</span></div>")
        lines.append(f"          <div><b style=\"font-size:18px\">Distributor: </b><span>{row[3]}</span></div>")
        
        #actor
        lines.append(f"          <div><b style=\"font-size:18px\">Cast: </b><span>")

        query_actor = f"SELECT a.name " \
                      f"FROM actors a, plays_in p " \
                      f"WHERE a.actor_id = p.actor_id " \
                      f"AND p.movie_id = {movie_id} " \
                      f"ORDER BY a.name ASC;"
        
        cursor.execute(query_actor)
        fetch_actors = cursor.fetchall()

        if len(fetch_actors):
            for actor in fetch_actors:
                lines.append(f"<span>{actor[0]}</span>")
                lines.append(f",")
            lines[-1:] = ""
        else:
            lines.append(f"N/A") 
          
        lines.append(f"</span></div>")
        
        #director
        lines.append(f"          <div><b style=\"font-size:18px\">Director(s): </b><span>")

        query_director = f"SELECT d.name " \
                         f"FROM directors d, directed dd " \
                         f"WHERE d.director_id = dd.director_id " \
                         f"AND dd.movie_id = {movie_id} " \
                         f"ORDER BY d.name;"
            
        cursor.execute(query_director)
        fetch_directors = cursor.fetchall()

        if len(fetch_directors):
            for director in fetch_directors:
                lines.append(f"<span>{director[0]}</span>")
                lines.append(f",")
            lines[-1:] = ""
        else:
            lines.append(f"N/A") 
            
        lines.append(f"</span></div>")
        
        #genre
        lines.append(f"          <div><b style=\"font-size:18px\">Genre(s): </b><span>")

        query_genre = f"SELECT g.genre_type, g.genre_id " \
                      f"FROM genres g, movie_genre m " \
                      f"WHERE g.genre_id = m.genre_id " \
                      f"AND m.movie_id = {movie_id} " \
                      f"ORDER BY g.genre_type ASC;"
        
        cursor.execute(query_genre)
        fetch_genres = cursor.fetchall()

        if len(fetch_genres):
            for genre in fetch_genres:
                lines.append(f"<span>{genre[0]}</span>")
                lines.append(f",")
            lines[-1:] = ""
        else:
            lines.append(f"N/A") 

        lines.append(   f"</span>"
                        f"  </div>"
                        f"      <div>"
                        f"          <b style=\"font-size:18px\">Rating: </b><span>{row[4]}</span></div>"
                        f"      <div>"
                        f"          <b style=\"font-size:18px\">Runtime: </b><span>{row[5]}</span></div>"
                        f"      <div>"
                        f"          <b style=\"font-size:18px\">Metascore: </b><span>{row[7]}</span>"
                        f"          <b style=\"font-size:18px\">\tUser Score: </b><span>{row[8]}</span></div>"
                        f"      <div>"
                        f"          <b style=\"font-size:18px\">Summary: </b><span>{row[6]}</span></div>"
                    )
        
        #links
        lines.append(f"          <div><b style=\"font-size:18px\">Stream Link(s): </b><span>")

        query_links = f"SELECT url FROM stream_links WHERE movie_id = {movie_id};"
        cursor.execute(query_links)
        
        fetch_links = cursor.fetchall()

        if len(fetch_links):
            for link in fetch_links:
                
                movie_link = link[0].split('/')[2]
                lines.append(f"<a href=\"{link[0]}\" target=\"_blank\">{movie_link}</a>")   
                lines.append(f", ")
            lines[-1:] = ""
        
        else:
            lines.append(f"N/A") 
            
        lines.append(f"</span></div>")  
        lines.append(f"     </div>")
        
    lines.append(f"</div><br></br>")
    lines.append('<hr class="solid">')
    lines.append(f"<div>")
    lines.append(f"</div><br></br>")

    # recommendation movie list 
    tags = ""
    for index, i in enumerate(sim_movies):
        
        print(f"sim_movies row = {i}")
        if index == 5:
            break
        tags += f'''
                    <div class="column" width=130 height=200>
                    <div><img src= \"{mc.get_img_from_title(i)}\" width=130 height=200 alt="poster"></div><br></br>
                    
                    <div><a style="display:block;width:350px" href = "{mc.get_img_from_title(i)}">{i}</a>
                    
                    
                    
                    
                            <form action=\"more\" method=\"post\"> "
                         <div>{i}</div> "
                         <div>"
                     <input style=\"height:30px;width:120px\" type=\"submit\" value=\"Show more info\">"
                      </div>"
                    <input type=\"hidden\" name =\"data\" value=\"{mc.get_id_from_title(i)}\">"
                   
        
                    
                    
                    
                    </div>
                </div>
                '''

    lines.append( f'''
                    <div class="container-fluid p-0">
                      <div><b style=\"font-size:26px\">Similar Movies</b></div>
                      <div class="img_row">
                        {tags}      
                    </div>
                    </div>
                    '''
                )
    
    lines.append('<hr class="solid">')
    #critic review
    lines.append(f"     <div style=\"width: 45%; float:left;\">")
    lines.append(f"<div><b style=\"font-size:26px\">Critic Reviews</b></div>")

    query_cr = f"SELECT c.c_review, c.c_score, c.polarity " \
               f"FROM critique cr, critic_reviews c " \
               f"WHERE cr.critic_id = c.critic_id " \
               f"AND cr.movie_id = {movie_id};"
               # f"AND cr.movie_id = {movie_id}  LIMIT 10;"
    
    cursor.execute(query_cr)
    fetch_cr = cursor.fetchall()
    if len(fetch_cr):
        for cr in fetch_cr:
            lines.append(f"<br>"
                         f" </br>"
                         f"     <div>"
                         f"         <b style=\"font-size:18px\">Review Score: </b>"
                         f"             <span>{cr[1]}</span>"
                         f"         <b style=\"font-size:18px\">\tReview Sentiment : </b>"
                    )
                            
            if cr[2] < -.3 :
                lines.append(f"                 <img src=\"/static/img/down.jpg\" alt=\"Negative\" width=18 height=18>")

            elif cr[2] < .3 :
                lines.append(f"                 <img src=\"/static/img/neutral.jpg\" alt=\"Neutral\" width=18 height=18>")

            else :
                lines.append(f"                 <img src=\"/static/img/up.jpg\" alt=\"Positive\" width=18 height=15>")
            
            lines.append(f"     <div>"
                         f"         <span>{cr[0]}</span>"
                         f"     </div>"
                         f"     </div>"
                        )
    else:
         lines.append(f"N/A")

    lines.append(f"     </div>")
    
    #user_review
    lines.append(f"     <div style=\"margin-left:45%;\">")
    lines.append(f"<div><b style=\"font-size:26px\">User Reviews</b></div>")

    query_ur = f"SELECT u.u_review, u.username, u.u_score, u.polarity " \
               f"FROM reviews r, user_reviews u " \
               f"WHERE u.review_id = r.review_id " \
               f"AND r.movie_id = {movie_id};"
               #f"AND r.movie_id = {movie_id}  LIMIT 10;"
    
    cursor.execute(query_ur)
    fetch_ur = cursor.fetchall()
    if len(fetch_ur):       
        for ur in fetch_ur:
            lines.append(   f"<br>"
                            f"  </br>"
                            f"      <div>"
                            f"          <b style=\"font-size:18px\">\tReview Score: </b><span>{ur[2]}</span>"
                            f"          <b style=\"font-size:18px\">\tReview Sentiment : </b>"   
                        )           
            if ur[3] is not None:
                if ur[3] < -.3 :
                    lines.append(f"                 <img src=\"/static/img/down.jpg\" alt=\"Negative\" width=18 height=18>")

                elif ur[3] < .3 :
                    lines.append(f"                 <img src=\"/static/img/neutral.jpg\" alt=\"Neutral\" width=18 height=18>")

                else :
                    lines.append(f"                 <img src=\"/static/img/up.jpg\" alt=\"Positive\" width=18 height=15>")

                lines.append(   f"          <br>"
                                f"              <b style=\"font-size:18px\">Reviewer Name: </b><span>{ur[1]}</span>"
                                f"          <div>"
                                f"              <span>{ur[0]}</span>"
                                f"          </div>"
                                f"      </div>"
                            )
        
    else:
         lines.append(f"N/A")
    
    lines.append(f"     </div>")

    lines.append(f"</body>")
    lines.append(f"</head>")
    lines.append("</html>")
    cursor.close()
    conn.close()
  
    return HttpResponse("\n".join(lines))

