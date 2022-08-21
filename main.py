import requests
import json
import pandas as pd
import numpy as np
import random
import time
from datetime import datetime as dt
from last_date_db import latest_date_db

from last_date_db import latest_date_db

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
               'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36', 
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36', 
               'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148', 
               'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36'] 

user_agent = random.choice(user_agents) 

headers = {
    'User-Agent': user_agent,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'trailers',
    'Content-Type': 'application/json',
    'accept': 'application/json'
}

list_url = ["https://pse.kominfo.go.id/static/json-static/ASING_TERDAFTAR/{}.json?page[page]={}&page[limit]=10&filter[search_term]=",
"https://pse.kominfo.go.id/static/json-static/ASING_DIHENTIKAN_SEMENTARA/{}.json?page[page]={}&page[limit]=10&filter[search_term]=",
"https://pse.kominfo.go.id/static/json-static/ASING_DICABUT/{}.json?page[page]={}&page[limit]=10&filter[search_term]=",
"https://pse.kominfo.go.id/static/json-static/LOKAL_DIHENTIKAN_SEMENTARA/{}.json?page[page]={}&page[limit]=10&filter[search_term]=",
"https://pse.kominfo.go.id/static/json-static/LOKAL_DICABUT/{}.json?page[page]={}&page[limit]=10&filter[search_term]=",
"https://pse.kominfo.go.id/static/json-static/LOKAL_TERDAFTAR/{}.json?page[page]={}&page[limit]=10&filter[search_term]="]

def main_data(start, end):
    ### start end must be integer
    max_date_db = latest_date_db() ## get last date from db
    df_results = pd.DataFrame()
    for i in range(start,end):
        page = 1
        page_json = page-1
        while True:
            url = list_url[i].format(page_json,page)
            response = requests.get(url, headers, timeout=60)
            time.sleep(5)
            try:
                result = response.json()
                metadata = pd.json_normalize(result['meta']['page'])
                totalpage = metadata.iloc[0]['lastPage']
                #get actual data
                df1 = pd.json_normalize(result['data'])
                df1['url'] = url
                df1['page'] = "page {}".format(page)
                tanggal_daftar = df1['attributes.tanggal_daftar'].unique().tolist()
                for td in tanggal_daftar:
                    if td == max_date_db:
                        df1 = df1[~df1['attributes.tanggal_daftar'] == max_date_db]
                    else:
                        pass
                df_results = pd.concat([df_results, df1], ignore_index=True)  
                if page < totalpage:
                    page += 1
                    page_json += 1
                else:
                    break
            except ValueError:
                print("ga ada datanya di {}".format(url))
                break

    df_results['updated_at'] = dt.now()
    df_results = df_results.drop_duplicates()
    df_results.columns = ["Type", "ID", "Nomor PB UMKU", "Nama Sistem", "Website", "Sektor", "Nama Perusahaan",
                        "Tanggal Daftar", "Nomor Tanda Daftar", "QR Code", "Status ID", "Sistem Elektronik ID", 
                        "URL API", "Page API", "updated_at"]
    df_results["Jenis Perusahaan"] = np.where(df_results["URL API"].str.contains("LOKAL"), "Lokal", "Asing")
    df_results["Tanggal Daftar"] = pd.to_datetime(df_results["Tanggal Daftar"])
    return df_results