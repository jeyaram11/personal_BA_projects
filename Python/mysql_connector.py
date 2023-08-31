# import all libaries
import yaml
import sqlalchemy

credentials_file = 'D:\personal_BA_projects\cred\credentials.yaml'


# function to create a connection to mysql server
def mysql_connection():
    credentials = yaml.safe_load(open(credentials_file))
    connection_to = 'mysqlserver'
    username = credentials[connection_to]['username']
    password = credentials[connection_to]['password']
    hostname = credentials[connection_to]['hostname']
    port = credentials[connection_to]['port']
    database = credentials[connection_to]['database']
    global engine

    try:
        engine = sqlalchemy.create_engine('mysql+mysqlconnector://{username}:{password}@{hostname}/{database}'
                                          .format(username=username,
                                                  password=password,
                                                  hostname=hostname,
                                                  database=database
                                                  ))
        connection = engine.connect()
        print("successfully connected")
        connection.close
    except Exception as e:
        print(e)


