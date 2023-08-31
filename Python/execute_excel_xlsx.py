import yaml
import pandas as pd
import dataframe_transform_functions as dtf
import mysql_connector as mc
import os
# create a function to obtain the xlsx file, transform into a dataframe, upload to sql.etl schema
source_path = 'D:\personal_BA_projects\cred\source_path.yaml'


def execute_xlsx(filename):
    # declare all the variables
    credentials = yaml.safe_load(open(source_path))
    connect_to = filename
    path = credentials[connect_to]['path']
    sheet = credentials[connect_to]['sheet']
    table_name = credentials[connect_to]['table_name']

    #open excel file and transfrom into dataframe
    #indexing can be removed using to_sql
    df = pd.read_excel(path,sheet_name=sheet)
    df = dtf.clean_columns(df)

    #create connection to mysql server
    engine = mc.mysql_connection()
    connection = engine.connect()

    #upload to mysql server
    try:
        df.to_sql(name='load_'+ table_name, con= engine, if_exists='replace',index=False)
        print('Successfully uploaded data')
    except Exception  as e:
        print('Failed ' + str(e))


execute_xlsx('movies')