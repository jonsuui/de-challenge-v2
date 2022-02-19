#from asyncio.windows_events import NULL
import pandas as pd
from os import listdir
from os.path import  abspath, dirname
from pathlib import Path
import json
import pandas as pd

class ETL:
    '''
    Class that performs extraction, transformation and load data into a report file 
    '''
    def __init__(self):
        print("loading...")
    '''
    Function that performs extraction from DATA folder 
    '''
    def extract(folder):
        df = pd.DataFrame()
        path = Path(dirname(abspath(__file__)))
        path = path.parent.parent.absolute() 
        files = listdir(str(path)+"\\"+folder)

        for file in files:
            f = open(str(path)+"\\data\\"+file)
            data = json.load(f)
            df_temp = pd.json_normalize(data)
            df_temp['season'] = file.replace('_json.json','')
            df_temp = pd.concat([df_temp,pd.get_dummies(df_temp['FTR'])], axis=1)
            frames = [df, df_temp]
            df = pd.concat(frames)

        return df
    '''
    Function that performs transformation of data, creating position table 
    '''
    def position_table_transform(df_results):

        position_table = df_results[['season','HomeTeam','AwayTeam','FTHG','FTAG', 'H', 'D','A', 'HY', 'HR','AY','AR']]
        position_table['H'] =  position_table['H'] *3
        position_table['A'] =  position_table['A'] *3
        position_table['HomePoints'] = position_table['H'] + position_table['D']
        position_table_home = position_table.groupby(['season', 'HomeTeam']).sum()
        position_table_home = position_table_home.rename(columns = {'HomeTeam': 'Team', 'HomePoints': 'Points','FTHG':'Goals','HR':'RedCards', 'HY':'YellowCards'}, inplace = False)
        position_table['AwayPoints'] = position_table['A'] + position_table['D']
        position_table_away = position_table.groupby(['season', 'AwayTeam']).sum()
        position_table_away = position_table_away.rename(columns = {'AwayTeam': 'Team', 'AwayPoints': 'Points','FTAG':'Goals','AR':'RedCards', 'AY':'YellowCards'}, inplace = False)
        frames = [position_table_home, position_table_away]
        seasons_position_table = pd.concat(frames)
        seasons_position_table = seasons_position_table.drop(['H', 'D', 'A', 'FTAG', 'AR','AY', 'FTHG', 'HR','HY', 'HomePoints'], axis=1)
        seasons_position_table = seasons_position_table.groupby(['season', 'HomeTeam']).sum()
        #pd.set_option("display.max_rows", None, "display.max_columns", None)
        

        return seasons_position_table
    '''
    Function that performs transformation of data, listing the dirtiest team for every season
    '''
    def dirtiest_team_transform(df_results):
        df_dirtiest = df_results.groupby(['season'], sort=False)['RedCards','YellowCards'].max()
        dff = pd.merge(df_results,df_dirtiest,on=['season','RedCards','YellowCards'])
        print(dff)
        #idx = df_results.index[df_dirtiest['RedCards'] == df_results['RedCards']]
        #print(idx)
        #return df_results[idx]#
        return df_results.groupby(['season'], sort=False)['RedCards','YellowCards'].max()
        #.sort_values(by=['season','Points'], ascending=(True,False))

    '''
    Function that performs transformation of data, listing the top socring teams by season 
    '''
    def top_scoring_team_transform(df_results):
        idx = df_results.groupby(['season'])['Goals'].transform(max) == df_results['Goals']
        return df_results[idx]
    
    '''
    Function that performs load of data, creating csv file for a dataframe, providing the name of the file 
    '''
    def load_data(df_results, name):
        path = Path(dirname(abspath(__file__)))
        path = path.parent.absolute()
        df_results.to_csv('{0}/output/{1}.csv'.format(str(path),name)) 


