import pandas as pd
import requests
from bs4 import BeautifulSoup

url = r'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures'
# df = pd.read_html(url)[0]

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('table')

links = []
for tr in table.findAll("tr"):
    trs = tr.findAll("td")
    for each in trs:
        try:
            link = each.find('a')['href']
            links.append(link)
        except:
            pass

df = pd.DataFrame(links)
df.columns = ['url']
# print(df.iloc[5].values)

df = df[df['url'].str.contains("Premier-League")]
df = df.drop_duplicates().reset_index(drop=True)
# print(df)
# print(df)
df_2 = pd.read_html(url)[0]
df_2 = df_2.dropna(how='all').reset_index(drop=True)

df_2 = df_2.join(df,how='inner')

cols = ['Wk','Date','Home','xG','Score','xG.1','Away','url']
df_2 = df_2[cols]

