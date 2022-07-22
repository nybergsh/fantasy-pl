import asyncio
import json
import requests

import aiohttp
import pandas as pd
import csv

from understat import Understat


async def main():
    df = pd.DataFrame()
    teams = ['Arsenal','Aston Villa','Brentford','Brighton','Burnley','Chelsea','Crystal Palace','Everton','Leeds','Leicester','Liverpool','Manchester City','Manchester United','Newcastle United','Norwich','Southampton','Tottenham','Watford','West Ham','Wolverhampton Wanderers']
    data = pd.read_csv(r'https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2022-23/id_dict.csv')

    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        for i,r in data.iterrows():
            grouped_stats = await understat.get_player_grouped_stats(r[0])
            
            df_tmp = pd.json_normalize(grouped_stats['season']).set_index(['season']).filter(like='2021',axis=0)
            df_tmp = df_tmp.loc[df_tmp['team'].isin(teams)]
            df_tmp = df_tmp[['games','time','goals','assists','xG','xA','yellow','red']]
            cols = df_tmp.columns
            df_tmp[cols] = df_tmp[cols].apply(pd.to_numeric, errors='coerce')


            df_tmp = df_tmp.agg(['sum'])
            df_tmp[['understat_code','fpl_id']] = [r[0],r[1]]
            df = pd.concat([df,df_tmp],ignore_index=True) 

    return df


def fpl_last_season():
    df = pd.DataFrame()
    data = pd.read_csv(r'https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2022-23/id_dict.csv')

    for i,r in data.iterrows():
        # get data from bootstrap-static endpoint
        
        try:
            req = requests.get(r'https://fantasy.premierleague.com/api/element-summary/'+str(r[1])+r'/').json()
            df_tmp = pd.json_normalize(req['history_past']).set_index(['season_name'])
            df_tmp = df_tmp.filter(like='2021/22',axis=0)
            df_tmp = df_tmp[['element_code','total_points','bps']]
            df_tmp['fpl_id'] = r[1]
            df = pd.concat([df,df_tmp],ignore_index=True)
        except:
            print('Could not fetch data for ' + str(r[1]) + ': ' + str(r[2]))
    print(df)
    return df

def fpl_current_state():
    
    # base url for all FPL API endpoints
    base_url = 'https://fantasy.premierleague.com/api/'

    # get data from bootstrap-static endpoint
    r = requests.get(base_url+'bootstrap-static/').json()
    
    df = pd.json_normalize(r['elements'])
    # print(df.columns)

    et_df = pd.json_normalize(r['element_types'])

    df = df.merge(et_df,left_on=['element_type'],right_on=['id'])

    team_df = pd.json_normalize(r['teams'])
    df = df.merge(team_df,left_on=['team_code'],right_on=['code'])
    
    df = df[['code_x','id_x','first_name','second_name','web_name','short_name','name','singular_name_short','singular_name','selected_by_percent','now_cost']]
    df[['selected_by_percent','now_cost']] = df[['selected_by_percent','now_cost']].apply(pd.to_numeric, errors='coerce')

    return df

def generate_data_model(main_df,understat_df,fpl_df):
    main_df = main_df.merge(understat_df,how='left',left_on=['id_x'],right_on=['fpl_id'])
    main_df = main_df.merge(fpl_df,how='left',left_on=['id_x'],right_on=['fpl_id'])
    
    main_df['ppg'] = main_df['total_points'] / main_df['games']
    main_df['vapm'] = ((main_df['ppg'] - 2) / main_df['now_cost']) *10
    print(main_df[['ppg','total_points','games','now_cost','vapm']])
    print(main_df.columns)

    main_df = main_df[['first_name','second_name','web_name','short_name','name','singular_name_short','singular_name','selected_by_percent','now_cost','ppg','vapm','bps','total_points','games','time']]

    main_df.to_csv(r'output/vapm_analysis.csv')

    return
# Main code

# Step 1: Load understat data for last season for all players
loop = asyncio.get_event_loop()
us_df = loop.run_until_complete(main())

# Step 2: Load fpl data for last season for all players
fpl_df = fpl_last_season()

# Step 3: Load current season elements w. support tables
elements_df = fpl_current_state()

# Step 4: Merge results into single table, clean, and store
generate_data_model(elements_df,us_df,fpl_df)