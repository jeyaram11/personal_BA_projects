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

  # create connection to mysql server
    engine = mc.mysql_connection()
    connection = engine.connect()

    # upload to mysql server
    try:
        df.to_sql(name='load_' + table_name, con=engine, if_exists='replace', index=False)
        print('Successfully uploaded data')
    except Exception as e:
        print('Failed ' + str(e))

    # add a truncate function takes data from load table and added to main table.
    # will come in handy when pulling last 30 days worth of data only
    with open(truncate_scripts + 'truncate_' + table_name + '.sql', 'r') as sql_script:
        truncate_script = sql_script.read()
    # with connection as cursor:
    #     #cursor.execute(sqlalchemy.text(truncate_script).execution_options(autocommit=True),multi)
    # connection.close()

    #can only execute single statements so using a for loop I executed each statement
    for statement in truncate_script.split(';'):
        if len(statement.strip()) > 0:

            connection.execute(sqlalchemy.text(statement + ';').execution_options(autocommit=True))

    print('Successfully executed truncate')
    connection.close()

    try:
        full_cmd_arguments = sys.argv
        user_input = full_cmd_arguments[1]

    except:
        user_input = input('Please enter filename:')

csv_update(user_input)
