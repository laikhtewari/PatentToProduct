import requests
import multiprocessing
import pandas as pd
import json
import string
import csv
import os
from datetime import datetime

##### MAKE SURE THESE PARAMS ARE APPROPRIATELY SET #####
curr_date = '0626_patch'
MAX_COLS = 10
API_MAX = 1000 # Up to 10K, but smthg smaller better else the API chokes up

base_path = "/home/users/ltewari/new_patent_scrape"
data_path = os.path.join(base_path, 'data')
input_path = os.path.join(base_path, 'input')
output_path = os.path.join(base_path, 'output', curr_date)

assignees_path = os.path.join(input_path, "assignee_names_patch.txt")
fields_path = os.path.join(input_path, "included_fields_" + curr_date + ".txt")
take_first_path = os.path.join(input_path, "fields_prefix_take_first.txt")

dump_dir = os.path.join(output_path, 'dumps_' + curr_date)
csv_dir = os.path.join(output_path, 'csv_' + curr_date)
########################################################

with open(fields_path) as f:
    attribute_list = f.readlines()
    attribute_list = [a.rstrip() for a in attribute_list]

attribute_string = '["{0}"]'.format('", "'.join(attribute_list))
patent_base_url = 'https://www.patentsview.org/api/patents/query?q={"assignee_organization":"%s"}&o={"per_page":%d,"page":%d}'
patent_url = patent_base_url + '&f=' + attribute_string

with open(take_first_path) as f:
    fields_prefix_take_first = f.readlines()
    fields_prefix_take_first = set([a.rstrip() for a in fields_prefix_take_first])

def format_filename(s):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ','_')
    return filename

def get_n_patents_for_assignee(assignee, n):
    clean_assignee = assignee.replace('&', '%26')
    pat_list = []
    tpc = -1
    page_num = 0
    while n > 0:
        print(n, "remaining to scrape for", assignee)
        page_num += 1
        url = patent_url % (clean_assignee, API_MAX, page_num)
        response = requests.get(url)
        if not response:
            print("API call failed on", assignee)
            return -1
        my_json = response.json()
        if my_json['patents'] == None:
            print("No patents received on", assignee)
            return -1
        pat_list += my_json['patents']
        tpc = my_json['total_patent_count']
        n -= API_MAX
    return {"patents":pat_list, "total_patent_count":tpc}

def get_num_patents(assignee):
    patents = get_n_patents_for_assignee(assignee, 1)
    if patents == -1:
        return -1
    if patents['total_patent_count'] == -1:
        print('No TPC received')
        return -1
    return patents['total_patent_count']

def build_csv_from_json(assignee, patents):
    row_list = []
    num_patents = len(patents['patents'])
    for i, patent_object in enumerate(patents['patents']):
        if (i % 100 == 0):
            print(f'Row {i} of {num_patents} for {assignee}')
                    
        row = {}
        for key in patent_object:
            if isinstance(patent_object[key], list):
                for j, elem in enumerate(patent_object[key]):
                    for sub_key in elem:
                        row[f'{sub_key}_{j}'] = elem[sub_key]
                    if j == 0 and sub_key.split('_')[0] in fields_prefix_take_first or j >= MAX_COLS - 1:
                        break
            else:
                row[key] = patent_object[key]
        row['assignee'] = assignee
        row_list.append(row)
    df = pd.DataFrame(row_list)
    filestring = format_filename(assignee)
    df.to_csv(os.path.join(csv_dir, f'{filestring}_patents.csv'))
    return df

def get_patents_for_assignee(assignee):
    print("Working on", assignee, "...")

    num_patents = get_num_patents(assignee)
    if num_patents == -1:
        print("ERROR: Failed to get number of patents on", assignee)
        return
    
    # if num_patents > 10000:
    #     print("ERROR: More than 10K patents on", assignee)
    #     return
            
    print(f'Downloading {num_patents} patents for', assignee)
    
    patents_json = get_n_patents_for_assignee(assignee, num_patents)
    if patents_json == -1:
        print("ERROR: Failed to get all patents for", assignee)
        return

    file_string = format_filename(assignee)
    with open(os.path.join(dump_dir, f'{file_string}_pats.txt'), 'w') as outfile:
        json.dump(patents_json, outfile)
    
    print('Scraped patents for', assignee)
    df = build_csv_from_json(assignee, patents_json)
    print('Done building', assignee)
    return df

if __name__ == '__main__':
    with open(assignees_path) as f:
        assignees = f.readlines()
        assignees = [a.rstrip() for a in assignees]

    t = datetime.now()        
    with multiprocessing.Pool(10) as p:
        df_list = p.map(get_patents_for_assignee, assignees)
        print('Finished scraping, took', datetime.now() - t, 'long')
        print('Joining data together...')
        t = datetime.now()
        master_df = pd.concat(df_list, sort=True)
        print('Finished joining, took', datetime.now() - t, 'long')
        print('Writing out...')
        master_df.to_csv(os.path.join(output_path, 'master_patents_raw_' + curr_date + '.csv'))

    print('goodbye :)')
