#python -m venv env
#imports
import classes.etl as etl

all_seasons_results = etl.ETL.extract("data")
position_table = etl.ETL.position_table_transform(all_seasons_results)
etl.ETL.load_data(position_table, 'position_table')
dirtiest_team = etl.ETL.dirtiest_team_transform(position_table)
etl.ETL.load_data(dirtiest_team, 'dirtiest_team')
top_scoring = etl.ETL.top_scoring_team_transform(position_table)
etl.ETL.load_data(top_scoring, 'top_scoring')
