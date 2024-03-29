# %%
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd

headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,it;q=0.7,es;q=0.6,gl;q=0.5',
        'cache-control': 'max-age=0',
        'dnt': '1',
        'referer': 'https://www.residentevildatabase.com/personagens/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }


def get_context(url):
    response = requests.get(url, headers=headers)
    return response

# %%
def get_basic_infos(soup):
    div_page = soup.find('div', class_='td-page-content')
    paragrafo = div_page.find_all('p')[1]
    ems = paragrafo.find_all('em')

    data = {}

    for i in ems:
        chave, valor, *_ = i.text.split(":")
        data[chave.strip()] = valor.strip()

    return data

# %%

def get_aparicoes(soup):
    lis = (soup.find('div', class_='td-page-content')
        .find('h4')
        .find_next()
        .find_all('li'))

    aparicoes = [i.text for i in lis]
    return aparicoes
# %%

def get_personagem_infos(url):

    response = get_context(url)

    if response.status_code != 200:
        print('Não foi possível obter os dados')
        return {}
    else:
        soup = BeautifulSoup(response.text)
        data = get_basic_infos(soup)
        data["Aparicoes"] = get_aparicoes(soup)
        return data

# %%
    
url = 'https://www.residentevildatabase.com/personagens/alex-wesker/'

get_personagem_infos(url)


# %%

def get_links():

    url = 'https://www.residentevildatabase.com/personagens'
    resp = requests.get(url, headers=headers)
    soup_personagens = BeautifulSoup(resp.text)
    ancoras = (soup_personagens.find('div', class_='td-page-content')
            .find_all('a'))
    links = [i['href'] for i in ancoras]
    return links


# %%

links = get_links()

data = [] 

for i in tqdm(links):

    try:
        d = get_personagem_infos(i)
        d['Link'] = i
        nome = i.strip('/').split("/")[-1].replace('-', ' ').title()
        d['Nome'] = nome

        data.append(d)
    except:
        pass

# %%


df = pd.DataFrame(data)

df.to_parquet('dados_re.parquet', index=False)

# %%

df_new = pd.read_parquet('dados_re.parquet')
df_new