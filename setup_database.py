import mysql.connector as mysql
import pandas as pd
import logging
import sys

db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "root",
    database = "movies"
)
cursor = db.cursor()

create_directors_command = """
create table if not exists directors(
  director_id varchar(255) primary key,
  director varchar(255)
)
"""

create_writers_command = """
create table if not exists writers(
  writer_id varchar(255) primary key,
  writer varchar(255)
)
"""

create_actors_command = """
create table if not exists actors(
  actor_id varchar(255) primary key,
  actor varchar(255)
)
"""

create_genres_command = """
create table if not exists genres(
  genre_id int not null auto_increment primary key,
  genre varchar(255)
)
"""

create_movies_command = """
create table if not exists movies(
  movie_id varchar(255) primary key,
  title varchar(255),
  director_id varchar(255),
  ranking int,
  rating float,
  year int,
  votesCount int,
  foreign key (director_id) references directors(director_id)
)
"""

create_movies_writers_command = """
create table if not exists movies_writers(
  id int not null auto_increment primary key,
  movie_id varchar(255),
  writer_id varchar(255),
  foreign key (movie_id) references movies(movie_id),
  foreign key (writer_id) references writers(writer_id),
  constraint movie_writer_uc unique (movie_id, writer_id)
)
"""

create_movies_actors_command = """
create table if not exists movies_actors(
  id int not null auto_increment primary key,
  movie_id varchar(255),
  actor_id varchar(255),
  foreign key (movie_id) references movies(movie_id),
  foreign key (actor_id) references actors(actor_id),
  constraint movie_actor_uc unique (movie_id, actor_id)
)
"""

create_movies_genres_command = """
create table if not exists movies_genres(
  id int not null auto_increment primary key,
  movie_id varchar(255),
  genre_id int,
  foreign key (movie_id) references movies(movie_id),
  foreign key (genre_id) references genres(genre_id),
  constraint movie_genre_uc unique (movie_id, genre_id)
)
"""

def create_databases():
  logger.info('Creating database')
  cursor.execute('drop database if exists movies_db')
  cursor.execute('create database movies_db')
  cursor.execute('use movies_db')

def create_tables():
  logger.info('Creating tables')
  cursor.execute(create_directors_command)
  cursor.execute(create_writers_command)
  cursor.execute(create_actors_command)
  cursor.execute(create_genres_command)
  cursor.execute(create_movies_command)
  
  cursor.execute(create_movies_writers_command)
  cursor.execute(create_movies_actors_command)
  cursor.execute(create_movies_genres_command)

def read_csv(csv_path):
  df = pd.read_csv(csv_path)
  headers = df.columns
  for i, row in df.iterrows():
    yield (tuple(headers), tuple(row))

def import_data():
  datasets = ['directors', 'genres', 'actors', 'writers', 'movies', 'movies_genres', 'movies_writers', 'movies_actors']
  for dataset in datasets:
    logger.info('Importing data for {} table'.format(dataset))
    for headers, row in read_csv('./dataset/{}.csv'.format(dataset)):
      str_headers = '(' + ','.join(headers) + ')'
      logger.info('insert into {}{} values {}'.format(dataset, str_headers, row))
      cursor.execute('insert into {}{} values {}'.format(dataset, str_headers, row))
      db.commit()

def setup_logger():
  logging.basicConfig(
      level=logging.INFO,
      format="%(asctime)s %(levelname)8s %(message)s",
      handlers=[
          logging.FileHandler("setup_database.log"),
          logging.StreamHandler(sys.stdout)
      ]
  )

  global logger
  logger = logging.getLogger("main")
  logger.setLevel(logging.INFO)

def main():
  setup_logger()
  create_databases()
  create_tables()
  import_data()

if __name__ == "__main__":
  main()
