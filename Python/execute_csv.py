"""
Execute file to extract files from csv and load to mysql server

"""
import pandas as pd
import yaml

#add file source path

# create a function to obtain the xlsx file, transform into a dataframe, upload to sql.etl schema
source_path = 'D:\personal_BA_projects\cred\source_path.yaml'


#connect to yaml file and retrieve credentails
def csv_update(filename):
    credentials = yaml.safe_load(open(source_path))
    connect_to = filename
    path = credentials[connect_to]['path']

#load csv file into a dataframe
    df = pd.read_csv(path)
    print(df)

#truncate file




#truncate and commit to mysql server
csv_update('emails')