# Script fetches data from fbref and FPL API and generates the data model used in data analysis
# Step by step:
# 1. Fetch all games from fbref
# 2. Generate team statistics from step 1 output
# 3. Fetch player data for each game between start_date and stop_date
# 4. Fetch FPL API data
# 5. Generate data model based on data in step 3 and step 4

# Modules
import pandas as pd
import numpy as np
import math
import requests
from bs4 import BeautifulSoup
import time
import datetime
from tqdm import tqdm

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

    team_map = pd.read_csv(r'../mapping_files/map_fbref_fpl_team.csv')

    df = df.merge(team_map,left_on='Home',right_on='Team_Name')
    df = df.merge(team_map,left_on='Away',right_on='Team_Name')

    return df



# Generate team statistics and store in matches.csv in output folder
def __generate_team_statistics(df):
    df_team_stats = pd.DataFrame()
    # For each row, generate one home team row and one away team row containing same information
    for i,r in tqdm(df.iterrows()):
        xCSH = math.exp(-r[5])
        xCSA = math.exp(-r[3])
        df = pd.DataFrame({
            'gw':[r[0],r[0]],
            'match_date':[r[1],r[1]],
            'home/away':['home','away'],
            'team_name':[r[8],r[11]],
            'team_id' : [r[9],r[12]],
            'team_code' : [r[10],r[13]],
            'opponent_name':[r[11],r[8]],
            'opponent_id' : [r[12],r[9]],
            'opponent_code' : [r[13],r[10]],
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
        df_team_stats = pd.concat([df_team_stats,df],ignore_index=True)
    
    df_team_stats.to_csv(r'../output/team_statistics.csv',index=False)
    return

def __fetch_fbref_player_data(games):
    base_url = r'https://fbref.com'
    df = pd.DataFrame()
    # games = games[]
    games = games[(games['Date'] >= '2022-08-05') & (games['Date'] <= '2022-08-05')]
    for i,r in tqdm(games.iterrows()):
        time.sleep(4)
        df_tmp = pd.read_html(base_url+r[7])
        df_home = df_tmp[3].dropna().droplevel(level=0,axis=1)
        df_home['team_id'] = r[9]
        df_home['opponent_id'] = r[12]
        df_home['gw'] = r[0]
        df_home['xCS'] = math.exp(-r[5])

        df_away = df_tmp[10].dropna().droplevel(level=0,axis=1)
        df_away['team_id'] = r[12]
        df_away['opponent_id'] = r[9]
        df_away['gw'] = r[0]
        df_away['xCS'] = math.exp(-r[3])
        df = pd.concat([df,df_home],ignore_index=True)
        df = pd.concat([df,df_away],ignore_index=True)
    
    player_map = pd.read_excel(r'../mapping_files/player_fbref_fpl.xlsx')
    df['player_key'] = df['Player'] + '_' + df['team_id'].astype(str)

    df = df.merge(player_map,how='left',left_on=['player_key'],right_on=['key'])
    df['played'] = 1
 
    cols = ['gw','Player','player_id','Pos','played','xG','npxG','xA','xCS','team_id','opponent_id']
    df = df[cols]
    df_missing = df[df['player_id'].isna()]
    df_missing.to_csv(r'../output/player_key_test.csv',index=False)
    return df

def __fpl_static_info(base):
    r = requests.get(base+'bootstrap-static/').json()
    df_el = pd.json_normalize(r['elements'])[['code','id','element_type','now_cost','web_name','selected_by_percent','team','team_code']].set_index('element_type')
    df_el_types = pd.json_normalize(r['element_types'])[['id','singular_name_short','singular_name']].set_index('id')
    df_el = df_el.join(df_el_types,how='inner')
    colnames = ['player_code','player_id','price','player_name','selected_by_percent','team_id','team_code','position_code','position_name']
    df_el.columns = colnames
    df_el = df_el.reset_index(drop=True)
    df_el.to_csv(r'../output/player_info.csv',index=False)
    df_teams = pd.json_normalize(r['teams'])[['code','id','name','short_name']]
    colnames = ['team_code','team_id','team_name','team_name_short']
    df_teams.columns = colnames
    df_teams.to_csv(r'../output/team_info.csv',index=False)

    return df_el


def __fetch_fpl_data(players):
    df = pd.DataFrame()
    base = r'https://fantasy.premierleague.com/api/'

    df_total_list =  __fpl_static_info(base)
    team_list = players['team_id'].drop_duplicates().values.tolist()
    # print(team_list)
    for tid in tqdm(team_list):
        df_tmp = df_total_list.loc[df_total_list['team_id'] == tid]
        player_list = df_tmp['player_id'].values.tolist()
        for pid in tqdm(player_list):
            r = requests.get(base+'element-summary/'+str(pid)+'/').json()
            df_tmp = pd.json_normalize(r['history'])
            df_tmp['team_id'] = tid
            df = pd.concat([df,df_tmp],ignore_index=True)
    
    stop = datetime.date(2022,8,5)
    start = datetime.date(2022,8,5)
    df['kickoff_time'] = df['kickoff_time'].astype('datetime64[ns]').dt.date
    df = df[(df['kickoff_time'] >= start) & (df['kickoff_time'] <= stop)]
    # player_list = players['player_id'].values.tolist()
    # for pid in tqdm(player_list):
    #     r = requests.get(base+'element-summary/'+str(pid)+'/').json()
    #     df_tmp = pd.json_normalize(r['history'])
    #     df = pd.concat([df,df_tmp],ignore_index=True)

    cols = ['element','fixture','minutes','total_points','round','goals_scored','assists','clean_sheets','goals_conceded','own_goals','penalties_saved','penalties_missed','yellow_cards','red_cards','saves','bonus','bps','influence','creativity','threat','ict_index','team_id','opponent_team']
    df = df[cols]
    colnames = ['player_id','fixture_id','minutes','points','gw','goals_scored','assists','clean_sheets','goals_conceded','own_goals','penalties_saved','penalties_missed','yellow_cards','red_cards','saves','bonus','bps','influence','creativity','threat','ict_index','team_id','opponent_id']
    df.columns = colnames

    df.to_csv(r'../output/fpl_player_test.csv',index=False)
    return df

def __update_facts_table(fbref,fpl):
    fbref = fbref.set_index(['player_id','gw','team_id','opponent_id'])
    fpl = fpl.set_index(['player_id','gw','team_id','opponent_id'])

    df = fbref.join(fpl,how='right')
    df = df.reset_index()

    df['xG'] = df['xG'].fillna(0)
    df['npxG'] = df['npxG'].fillna(0)
    df['xA'] = df['xA'].fillna(0)
    df['xCS'] = df['xCS'].fillna(0)
    df['played'] = df['played'].fillna(0)
    df = df.drop(columns='Player')
    print(df)
    print(df.columns)
    df.to_csv(r'../output/player_statistics.csv',index=False)
    return


if __name__ == "__main__":
    # 1. Fetch all games w. game links from fbref
    df_matches = __fetch_fbref_match_list()
    
    # 2. Create team statistics csv file
    __generate_team_statistics(df_matches)

    # 3. Fetch player data between start date and stop date
    df_pl_fbref = __fetch_fbref_player_data(df_matches)

    # 4. Fetch FPL data
    df_pl_fpl_api = __fetch_fpl_data(df_pl_fbref)

    # 5. Generate and store facts table model
    __update_facts_table(df_pl_fbref,df_pl_fpl_api)