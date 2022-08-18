import requests
import pandas as pd
from tqdm import tqdm

from google.cloud.sql.connector import Connector
from google.cloud import storage
import pymysql
import sqlalchemy

def __fetch_static_tables(base):

    r = requests.get(base+'bootstrap-static/').json()
    df_el = pd.json_normalize(r['elements'])[['code','id','element_type','team','team_code','now_cost','web_name','selected_by_percent']]
    df_teams = pd.json_normalize(r['teams'])[['code','id','name','short_name','played','win','draw','loss','position','points']]
    df_types = pd.json_normalize(r['element_types'])[['id','singular_name','singular_name_short','plural_name']]

    return df_el,df_teams,df_types

def __fetch_player_details(base,player_list):
    print('Fetching gw data for each player in db')
    df = pd.DataFrame()
    players = player_list['id'].values.tolist()
    for id in tqdm(players):
        r = requests.get(base+'element-summary/'+str(id)+'/').json()
        df_tmp = pd.json_normalize(r['history'])

        df = pd.concat([df,df_tmp],ignore_index=True)
    
    cols = ['element','fixture','opponent_team','round','minutes','goals_scored','assists','clean_sheets','goals_conceded','own_goals','penalties_saved','penalties_missed','yellow_cards','red_cards','saves','bonus','bps','ict_index','selected']
    df = df[cols]
    colnames = ['player_id','fixture_id','opponent_id','gw','minutes','goals_scored','assists','clean_sheets','goals_conceded','own_goals','penalties_saved','penalties_missed','yellow_cards','red_cards','saves','bonus','bps','ict_index','selected']
    
    df['ict_index'] = pd.to_numeric(df['ict_index'],errors='coerce')
    df.columns = colnames

    df['player_fixture_id'] = df['player_id'].astype(str) + '_' + df['gw'].astype(str) + '_' + df['opponent_id'].astype(str)

    return df

def __export_fpl_to_sql(df_el,df_teams,df_element_types,df_players):
 # initialize Connector object
    connector = Connector()

    # function to return the database connection
    def getconn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = connector.connect(
            "awesome-lotus-357213:europe-north1:fpl-db",
            "pymysql",
            user="root",
            password="9f5de36d-92d7-4073-b270-41d26db2f29c",
            db="fpl"
        )
        return conn

    # create connection pool
    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
    )


    with pool.begin() as con:
        
        # Straight export of statics using pandas function
        df_el.to_sql('players',con=pool,if_exists='replace',index=False)
        df_teams.to_sql('teams',con=pool,if_exists='replace',index=False)
        df_element_types.to_sql('element_types',con=pool,if_exists='replace',index=False)
        df_players.to_sql('player_gw',con=pool,if_exists='replace',index=False)
       
        # con.close()
    return


def __export_fpl_to_csv(df_el,df_teams,df_element_types,df_players):
    df_el.to_csv(r'../output/elements.csv',index=False)
    df_teams.to_csv(r'../output/teams.csv',index=False)
    df_element_types.to_csv(r'../output/positions.csv',index=False)
    df_players.to_csv(r'../output/player_gw.csv',index=False)
    return


def fpl_api_main():

    base_url = 'https://fantasy.premierleague.com/api/'
    print('Extracting data from static tables')
    elements,teams,element_types = __fetch_static_tables(base_url)

    print('Extracting player gameweek data')
    player_data = __fetch_player_details(base_url,elements)

    print('Exporting data to MySQL db')
    __export_fpl_to_sql(elements,teams,element_types,player_data)

    # print('Uploading to CSV')
    # __export_fpl_to_csv(elements,teams,element_types,player_data)

    return
