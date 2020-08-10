import pandas as pd 
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
import multiprocessing
import os

write_path = '/home/users/ltewari/scac_lit/'
base_url = 'http://securities.stanford.edu/filings.html?page=%d'
max_pages = 290
num_processors = 10

def pull_table_on_page(page_num):
  response = requests.get(base_url % page_num)
  if not response:
    print('Failed on page', page_num)
  my_table = bs(response.content, 'html.parser').find_all('table')[0]
  
  headers = []
  for th in my_table.find('tr').find_all('th'):
    headers.append(th.text.strip())

  rows = []
  for tr in my_table.find_all('tr')[1:]:
    cells = []
    tds = tr.find_all('td')
    rows.append([td.text.strip() for td in tds])

  return(pd.DataFrame(rows, columns=headers))

if __name__ == '__main__':
  t = datetime.now()
  with multiprocessing.Pool(num_processors) as p:
    df_list = p.map(pull_table_on_page, list(range(1, max_pages + 1)))
    print('Finished scraping, took', datetime.now() - t, 'long')
    print('Joining...')
    t = datetime.now()
    full_df = pd.concat(df_list, ignore_index=True)
    print('Done Joining. Writing out...')
    full_df.to_csv(os.path.join(write_path, 'scac_filings.csv'))
