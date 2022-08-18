from datetime import datetime
import time
import pandas as pd
import datetime
from os.path import exists

from google.cloud.sql.connector import Connector
import pymysql
import sqlalchemy



def map_player_id(df):
    map = pd.read_excel(r'../mapping_files/player_fbref_fpl.xlsx')
    df['player_key'] = df['Player'] + '_' + df['team_id'].astype(str)
    df = df.merge(map,how='left',left_on=['player_key'],right_on=['key'])

    return df

def add_key_fields_to_fbref(df,rows,id,opponent,gw,gdt):
    rows = rows.droplevel(level=0,axis=1)
    rows['team_id'] = id
    rows['opponent_id'] = opponent
    rows['gw'] = gw

    rows = map_player_id(rows)

    rows['key'] = rows['player_id'].astype(str) + '_' + str(gw) + '_' + str(opponent)
    rows = rows.drop(['player_key'],axis=1)
    rows['match_date'] = gdt
    df = pd.concat([df,rows],ignore_index=True)
    return df

def fetch_games(start,stop,today):
    df = pd.DataFrame()

    while start < stop: 
        try:
            df_list = pd.read_excel(r'../mapping_files/fbref_links.xlsx')
            df_list['Match_Date'] = pd.to_datetime(df_list['Match_Date']).dt.date
            df_list.set_index('Match_Date',inplace=True)
            df_list = df_list.loc[[start]]
            print(str(len(df_list.index))+ ' game(s) on ' + str(start))

            cntr = 1
            for i,r in df_list.iterrows():
                url_code = r[3]
                df_tmp = pd.read_html(r'https://fbref.com/en/matches/'+str(url_code))

                df = add_key_fields_to_fbref(df,df_tmp[3].dropna(),r[4],r[5],r[0],start)
                df = add_key_fields_to_fbref(df,df_tmp[10].dropna(),r[5],r[4],r[0],start)
                
                print(str(cntr)+' complete')
                time.sleep(3)
                cntr += 1

        except KeyError as err:
            print('no games on ' + str(start))
        except BaseException as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise
        start += datetime.timedelta(days=1)
    if len(df.index) > 0:
        df['extract_date'] = today

        cols = ['Player','Pos','Min','Gls','Ast','xG','npxG','xA','team_id','key','player_id','extract_date','match_date','opponent_id','gw']
        
        df = df[cols]
    
    return df

def export_fbref_to_sql(df):
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
        # Import new games to temporary table in SQL
        df.to_sql('fbref_tmp',con=pool,if_exists='replace',index=False)

        # Add new games to merged table. Duplicates are updated with info from new 
        sql_insert = """
            INSERT INTO `fbref`(`key`,
                `player_id`,
                `team_id`,
                `opponent_id`,
                `gw`,
                `extract_date`,
                `match_date`,
                `Player`,
                `Pos`,
                `Min`,
                `Gls`,
                `Ast`,
                `xG`,
                `npxG`,
                `xA`)
            SELECT `key`,
                `player_id`,
                `team_id`,
                `opponent_id`,
                `gw`,
                `extract_date`,
                `match_date`,
                `Player`,
                `Pos`,
                `Min`,
                `Gls`,
                `Ast`,
                `xG`,
                `npxG`,
                `xA`
            FROM `fbref_tmp`
            ON DUPLICATE KEY UPDATE 
                `fbref`.`team_id` = `fbref_tmp`.`team_id`,
                `fbref`.`extract_date` = `fbref_tmp`.`extract_date`,
                `fbref`.`match_date` = `fbref_tmp`.`match_date`,
                `fbref`.`Pos` = `fbref_tmp`.`Pos`,
                `fbref`.`Min` = `fbref_tmp`.`Min`,
                `fbref`.`Gls` = `fbref_tmp`.`Gls`,
                `fbref`.`Ast` = `fbref_tmp`.`Ast`,
                `fbref`.`xG` = `fbref_tmp`.`xG`,
                `fbref`.`npxG` = `fbref_tmp`.`npxG`,
                `fbref`.`xA` = `fbref_tmp`.`xA`
            ;
        """

        con.execute(sql_insert)

        # Drop temp table
        con.execute("""DROP TABLE IF EXISTS fbref_tmp;""")

        # con.close()
    
    return

def export_fbref_to_csv(df):
    filepath = r'../output/fbref.csv'
    if exists(filepath):
        df_2 = pd.read_csv(filepath)

    return

def fbref_main():

    extract_date = datetime.date(2022,8,5)
    stop_date = datetime.date.today()
    today_date = datetime.date.today()

    # extract_date = stop_date - datetime.timedelta(days=1)

    fbref_games = fetch_games(extract_date,stop_date,today_date)
    
    if len(fbref_games.index) > 0:
        print('Adding {} rows to fpl.fbref and csv file'.format(len(fbref_games.index)))
        export_fbref_to_sql(fbref_games)

        # export_fbref_to_csv(fbref_games)
        # fbref_games.to_csv(r'../output/fbref.csv')

    else:
        print('No new rows to add')

    return

    

    