#%%

import requests
import datetime
import json
import pandas as pd
import time

#%%

class Collector:

    def __init__(self, url, instance_name, save_format) -> None:
        self.url = url
        self.instance_name = instance_name
        self.save_format = save_format
        

    def get_content(self, **kwargs):
        resp = requests.get(url=self.url, params=kwargs)
        return resp


    def save_parquet(self, data):
        df = pd.DataFrame(data)
        df.to_parquet(f'/data/{self.instance_name}/parquet/{self.now}.parquet', index=False)


    def save_json(self, data):
        with open(f'data/{self.instance_name}/json/{self.now}.json', 'w') as open_file:
                        json.dump(data, open_file, indent=4)


    def save_data(self, data, save_format):
        self.now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S%f')
     
        if self.save_format == 'json':
            self.save_json(data)
        elif self.save_format == 'parquet':
            self.save_parquet(data)


    def get_and_save(self, save_format='json', **kwargs):
         
        resp = self.get_content(**kwargs)

        if resp.status_code == 200:
            data = resp.json()
            self.save_data(resp.json(), self.save_format)
        else:
            data = None
            print(f'Request sem sucesso. {resp.status_code}', resp.json())
        
        return data

    def auto_exec(self, date_stop='2019-01-01'):
        page = 1
        while True:
            print(page)
            data = self.get_and_save(
                save_format=self.save_format, 
                page=page, 
                per_page=100
            )

            if data == None:
                print(f'Erro ao coletar dados.\nAguardando 10 segundos.')
                time.sleep(10)
            else:
                date_last = pd.to_datetime(data[-1]['published_at']).date()
                if date_last < pd.to_datetime(date_stop).date():
                    print(date_last)
                    break
                elif len(data) < 100:
                    print(len(data))
                    break

                page +=1
                time.sleep(5)
                




#%%
              
url = 'https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/'

collect = Collector(url, 'episodes', 'json')

#%%

#collect.get_content().json()


# %%

#collect.get_and_save()
# %%

collect.auto_exec()
# %%
