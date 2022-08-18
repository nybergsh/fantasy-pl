# Script fetches data from fbref and FPL API and generates the data model used in data analysis
# Step by step:
# 1. Fetch all games from fbref
# 2. Generate team statistics from step 1 output
# 3. Fetch player data for each game between start_date and stop_date
# 4. Fetch FPL API data
# 5. Generate data model based on data in step 3 and step 4

# Modules
import pandas as pd
import math
import requests
from bs4 import BeautifulSoup


# Fetches the fixture/result overview from fbref and returns a df with all played games current season, + URL to match report
def __fetch_fbref_match_list():
    url = r'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures'
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    
    # Fetching urls in table
    links = []
    for tr in table.findAll("tr"):
        trs = tr.findAll("td")
        for each in trs:
            try:
                link = each.find('a')['href']
                links.append(link)
            except:
                pass
    df_url = pd.DataFrame(links)
    df_url.columns = ['match_link']

    # Eliminating everything but match report links
    df_url = df_url[df_url['match_link'].str.contains("Premier-League")].drop_duplicates().reset_index(drop=True)

    # Loading raw data as df
    df = pd.read_html(url)[0]

    # Drop all-na rows and reset index
    df = df.dropna(how='all').reset_index(drop=True)

    # Inner join all match report links
    df = df.join(df_url,how='inner')

    # Only keep relevant columns
    keep_cols = ['Wk','Date','Home','xG','Score','xG.1','Away','match_link']
    df = df[keep_cols]

    return df



# Generate team statistics and store in matches.csv in output folder
def __generate_team_statistics(df):
    df_team_stats = pd.DataFrame()
    # For each row, generate one home team row and one away team row containing same information
    for i,r in df.iterrows():
        xCSH = math.exp(-r[5])
        xCSA = math.exp(-r[3])
        df = pd.DataFrame({
            'gw':[r[0],r[0]],
            'match_date':[r[1],r[1]],
            'home/away':['home','away'],
            'fbref_team':[r[2],r[6]],
            'fbref_opponent':[r[6],r[2]],
            'xGF':[r[3],r[5]],
            'xGA':[r[5],r[3]],
            'GF': [r[4][0],r[4][-1]],
            'GA': [r[4][-1],r[4][0]],
            'xCS': [xCSH,xCSA]
            })
        df['GA']=pd.to_numeric(df['GA'],errors='coerce')
        df['GF']=pd.to_numeric(df['GF'],errors='coerce')
        df['gw'] = df['gw'].astype(int)
        df['match_date'] = df['match_date'].astype('datetime64[ns]').dt.date
        # df['xCS'] = math.exp(-df['xGA'])
        df_team_stats = pd.concat([df_team_stats,df],ignore_index=True)
    
    df_team_stats.to_csv(r'../output/team_statistics.csv',index=False)

    return

if __name__ == "__main__":
    df_matches = __fetch_fbref_match_list()
    print(df_matches)
    __generate_team_statistics(df_matches)

    