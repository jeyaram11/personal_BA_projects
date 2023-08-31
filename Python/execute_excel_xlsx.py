import yaml
import pandas as pd
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
    df = pd.read_excel(path,sheet_name=sheet)
    return df


print(execute_xlsx('movies'))