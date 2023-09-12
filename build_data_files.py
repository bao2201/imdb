from operator import index
import pandas as pd

def extract_movie_dataset(df):
  movies_df = df[['movie_id', 'ranking', 'title', 'year', 'rating', 'votesCount', 'director_id']]
  movies_df.to_csv('./dataset/movies.csv', index=False)

def extract_genres_dataset(df):
  genres_df = df[['genres']].explode('genres').drop_duplicates()
  genres_df['genre_id'] = range(1, len(genres_df) + 1)
  genres_df.rename(columns={'genres': 'genre'}, inplace=True)
  genres_df.to_csv('./dataset/genres.csv', index=False)

def extract_director_dataset(df):
  directors_df = df[['director_id', 'director']].drop_duplicates()
  directors_df.to_csv('./dataset/directors.csv', index=False)

def extract_writers_dataset(df):
  writers = df[['writers']].explode('writers').drop_duplicates()
  writers[['writer', 'writer_id']] = pd.DataFrame(writers.writers.tolist(), index=writers.index)
  writers.drop(['writers'], axis=1).to_csv('./dataset/writers.csv', index=False)

def extract_actors_dataset(df):
  actors = df[['actors']].explode('actors').drop_duplicates()
  actors[['actor', 'actor_id']] = pd.DataFrame(actors.actors.tolist(), index=actors.index)
  actors.drop(['actors'], axis=1).to_csv('./dataset/actors.csv', index=False)

def extract_movies_genres_dataset(df):
  genres_df = df[['genres']].explode('genres').drop_duplicates()
  genres_df['genre_id'] = range(1, len(genres_df) + 1)
  movies_genres_df = df[['movie_id', 'genres']].explode('genres')
  movies_genres_df = pd.merge(movies_genres_df, genres_df, on=['genres'])
  movies_genres_df['id'] = range(1, len(movies_genres_df) + 1)
  movies_genres_df.drop(['genres'], axis=1, inplace=True)
  movies_genres_df.to_csv('./dataset/movies_genres.csv', index=False)

def extract_movies_writers_dataset(df):
  writers = df[['writers']].explode('writers').drop_duplicates()
  writers[['writer', 'writer_id']] = pd.DataFrame(writers.writers.tolist(), index=writers.index)
  movies_writers_df = df[['movie_id', 'writers']].explode('writers')
  movies_writers_df = pd.merge(movies_writers_df, writers, on=['writers'])
  movies_writers_df.drop(['writers', 'writer'], axis=1, inplace=True)
  movies_writers_df['id'] = range(1, len(movies_writers_df) + 1)
  movies_writers_df.to_csv('./dataset/movies_writers.csv', index=False)

def extract_movies_actors_dataset(df):
  actors = df[['actors']].explode('actors').drop_duplicates()
  actors[['actor', 'actor_id']] = pd.DataFrame(actors.actors.tolist(), index=actors.index)
  movies_actors_df = df[['movie_id', 'actors']].explode('actors')
  movies_actors_df = pd.merge(movies_actors_df, actors, on=['actors'])
  movies_actors_df.drop(['actors', 'actor'], axis=1, inplace=True)
  movies_actors_df['id'] = range(1, len(movies_actors_df) + 1)
  movies_actors_df.to_csv('./dataset/movies_actors.csv', index=False)

def main():
  df = pd.read_pickle('./raw/movies')
  extract_movie_dataset(df)
  extract_genres_dataset(df)
  extract_director_dataset(df)
  extract_writers_dataset(df)
  extract_actors_dataset(df)

  extract_movies_genres_dataset(df)
  extract_movies_writers_dataset(df)
  extract_movies_actors_dataset(df)

if __name__ == "__main__":
  main()