import pandas as pd 
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
import multiprocessing
import os
from math import ceil
import re

### SET THESE ###
write_path = '/home/users/ltewari/scac_lit/'
num_processors = 20
#################
base_url = 'http://securities.stanford.edu/'
table_url = base_url + 'filings.html?page=%d'
rows_per_page = 20 # This can technically change on the website so be careful

def pull_extended_data(url, page_num):
  new_data = []
  res = requests.get(url)
  if not res:
    print('Failed to get extended data at', url)

  ps = bs(res.content, 'html.parser')

  sector_finder = re.compile(r'<strong>Sector:</strong>\s+(\w+(?:\s+\w+)*)')
  sector_re = sector_finder.search(res.text)
  sector = sector_re.groups()[0] if sector_re is not None else None
  new_data.append(sector)

  industry_finder = re.compile(r'<strong>Industry:</strong>\s+(\w+(?:\s+\w+)*)')
  industry_re = industry_finder.search(res.text)
  industry = industry_re.groups()[0] if industry_re is not None else None
  new_data.append(industry)

  hq_finder = re.compile(r'<strong>Headquarters:</strong>\s+(\w+(?:\s+\w+)*)')
  hq_re = hq_finder.search(res.text)
  hq = hq_re.groups()[0] if hq_re is not None else None
  new_data.append(hq)

  judge_finder = re.compile(r'<strong>JUDGE:</strong>\s+(\w+.*(?:\s+\w.*)*)</div>')
  judge_re = judge_finder.search(res.text)
  judge = judge_re.groups()[0] if judge_re is not None else None
  new_data.append(judge)

  ol = ps.find_all('ol')
  plaintiff_list = ol[0] if ol is not None else []
  plaintiff_string = ';'.join([t.text.strip() for t in plaintiff_list.find_all('li')])
  new_data.append(plaintiff_string)
  
  return new_data

def pull_table_on_page(page_num):
  print(f'Working on {page_num}')
  response = requests.get(table_url % page_num)
  if not response:
    print('Failed on page', page_num)
  my_table = bs(response.content, 'html.parser').find_all('table')[0]
  
  headers = []
  for th in my_table.find('tr').find_all('th'):
    headers.append(th.text.strip())

  # headers.append('case_summary')
  headers.append('sector')
  headers.append('industry')
  headers.append('headquarters')
  # headers.append('first_identified_complaint')
  headers.append('judge')
  headers.append('plaintiff_firms')
  # headers.append('case_link')

  extended_url_finder = re.compile('filings-case.html\?id=\d+')
  
  rows = []
  for tr in my_table.find_all('tr')[1:]:
    cells = []
    tds = tr.find_all('td')
    row_data = [td.text.strip() for td in tds]
    extended_url = base_url + extended_url_finder.search(tr['onclick']).group()
    rows.append(row_data + pull_extended_data(extended_url, page_num))

  print(f'Done with {page_num}')

  return(pd.DataFrame(rows, columns=headers))

if __name__ == '__main__':
  print('Finding number of rows to pull...')
  numrow_finder = finder = re.compile(r'Securities Class Action Filings \((\d+)\)')
  init_res = requests.get(table_url % 1)
  if not init_res:
    raise('Could not get number of rows')
  num_rows = int(finder.search(init_res.text).groups()[0])
  max_pages = ceil(num_rows / rows_per_page)
  print(f'Pulling {num_rows} rows from {max_pages} pages...')

  t = datetime.now()
  with multiprocessing.Pool(num_processors) as p:
    df_list = p.map(pull_table_on_page, list(range(1, max_pages + 1)))
    print('Finished scraping, took', datetime.now() - t, 'long')
    print('Joining...')
    t = datetime.now()
    full_df = pd.concat(df_list, ignore_index=True)
    print('Done Joining. Writing out...')
    full_df.to_csv(os.path.join(write_path, 'scac_filings_0831.csv'), index=False)
