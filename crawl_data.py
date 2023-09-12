#https://bit.ly/2NyxdAG
from bs4 import BeautifulSoup
import requests
import re
import csv
import json
import logging
import sys
import pandas as pd

imdb_url = 'https://www.imdb.com'
top_250_movies_url = '{}/chart/top'.format(imdb_url)

def to_csv(data, file_name):
  keys = data[0].keys()
  with open(file_name, 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(data)

def is_number(s):
  try:
    float(s)
    return True
  except ValueError:
    return False

def load(url, headers = {}):
  response = requests.get(url, headers=headers)
  soup = BeautifulSoup(response.text, 'html.parser')
  return soup

def top_movie_links():
  soup = load(top_250_movies_url)
  links = ['{}{}'.format(imdb_url, a.attrs.get('href')) for a in soup.select('td.titleColumn a')]
  return links

def read_movie_detail_page(url, rank, total):
  logger.info('Crawling url: {} - {}/{}'.format(url, rank, total))
  headers = { 'Accept-Language': 'en-US' }
  soup = load(url, headers=headers)
  title = [v.get_text() for v in soup.select('.sc-b73cd867-0')][0]
  movie_id = url.replace('{}/title/'.format(imdb_url), '')[:-1]
  year = [v.get_text() for v in soup.select('.sc-8c396aa2-1') if is_number(v.get_text())][0]
  rating = [v.get_text() for v in soup.select('.sc-7ab21ed2-1') if is_number(v.get_text())][0]
  genres = [v.get_text() for v in soup.select('.sc-16ede01-3')]
  votesCount = json.loads(soup.find('script', type='application/ld+json').text)['aggregateRating']['ratingCount']

  director_tab = soup.select('.sc-fa02f843-0')[0].ul.li
  writers_tab = director_tab.find_next_sibling()
  actors_tab = writers_tab.find_next_sibling()
  
  director = director_tab.div.find('a').get_text()
  director_url = director_tab.div.find('a')['href']
  director_id = re.search('^/name/(.+)/', director_url).group(1)
  
  writers = [v.get_text() for v in writers_tab.div.find_all('a')]
  writers_url = [v['href'] for v in writers_tab.div.find_all('a')]
  writers_id = [re.search('^/name/(.+)/', v).group(1) for v in writers_url]

  actors = [v.get_text() for v in actors_tab.div.find_all('a')]
  actors_url = [v['href'] for v in actors_tab.div.find_all('a')]
  actors_id = [re.search('^/name/(.+)/', v).group(1) for v in actors_url]

  return { 
    'title': title,
    'movie_id': movie_id,
    'year': int(year),
    'rating': float(rating),
    'votesCount': int(votesCount),
    'genres': genres,
    'director': director,
    'director_id': director_id,
    'writers': [(p, id) for p, id in zip(writers, writers_id)],
    'actors': [(p, id) for p, id in zip(actors, actors_id)],
    'ranking': rank,
  }

def setup_logger():
  logging.basicConfig(
      level=logging.INFO,
      format="%(asctime)s %(levelname)8s %(message)s",
      handlers=[
          logging.FileHandler("crawl_data.log"),
          logging.StreamHandler(sys.stdout)
      ]
  )

  global logger
  logger = logging.getLogger("main")
  logger.setLevel(logging.INFO)

def main():
  setup_logger()
  links = top_movie_links()
  logger.info('Start collecting data for {} movies'.format(len(links)))
  rows = [read_movie_detail_page(url, idx + 1, len(links)) for idx, url in enumerate(links)]
  df = pd.DataFrame(rows)
  df.to_pickle("./raw/movies")

if __name__ == "__main__":
  main()
