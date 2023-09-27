import pandas as pd
import requests
import sqlalchemy
import yaml
import os
import hashlib
import json


#obtain the file name
source_path_name = os.path.splitext(os.path.basename(__file__))[0]

#load and open credentails 
credentials_file = 'credentials.yaml'
credentials = yaml.safe_load(open(credentials_file))
#load and open file path
paths_file = 'source_path.yaml'
paths = yaml.safe_load(open(paths_file))


#access database credentails
cred_type = 'mkt_dwh'
db = credentials[cred_type]['db']
user = credentials[cred_type]['user']
host = credentials[cred_type]['host']
pw = credentials[cred_type]['pw']

# connect to server
engine = sqlalchemy.create_engine("postgresql://{user}:{pw}@{host}/{db}"
                                  .format(host=host,
                                          db=db,
                                          user=user,
                                          pw=pw))
#connect to server 
try:
    connection = engine.connect()
    print('connected to server')
except:
    print('not connected to server')
    raise

#run the script to find daily records that needs to be sent
script_name = paths[source_path_name]['source']
dwh_table = paths[source_path_name]['dwh_table']
script = open(f"sql_code/{script_name}.sql",'r')
script_code = script.read()
dataset = pd.read_sql_query(script_code,engine)

#convert to a dataframe
df = pd.DataFrame(dataset)


#apply sha256 encryption before posting data
df['phone_number'] = df['original_phone'] 
df['phone_number'] = df['original_phone'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
# Convert the 'date_column' to datetime objects with the original format
df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d %H:%M:%S')

# Convert the datetime objects to ISO 8601 format, this is the format accepted by API endpoint 
df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

#obtain endpoint and bearer token

endpoint = credentials[source_path_name]['Endpoint']
bearer_token = credentials[source_path_name]['Bearer Token Authentication']
client_id = credentials[source_path_name]['client_id']
event_name = credentials[source_path_name]['event_name']
action_source = credentials[source_path_name]['action_source']
action_source_url = credentials[source_path_name]['action_source_url']
delivery_optimization = credentials[source_path_name]['delivery_optimization']
pixel_id = credentials[source_path_name]['pixel_id']
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + bearer_token
    }

#create an empty list to store sent recored, we do not want to grow dataframe, solution is to grow list then create a dataframe
#from the list
appended_data = []

records = 0

 
#for each record
for index, record in df.iterrows():
    payload = json.dumps({
        'client_id': client_id,
        'event_name': event_name,
        'event_time': record['created_at'],
        'event_id': record['fbid'],
        'action_source': action_source,
        'action_source_url': action_source_url,
        'delivery_optimizaiton': delivery_optimization,
        'customer': {
           'phone_number': record['original_phone'],
           'pixel_id': pixel_id,
           'click_id': record['ndclid'],
           'client_ip_address': record['client_ip']
            }}
        )
    try:
        r = requests.request("POST",endpoint, headers=headers, data=payload)
        data = [record['lead_id'],record['created_at'],record['phone_number'],record['original_phone'],str(r.status_code),str(r.content)]
        appended_data.append(data)
        records += 1
    except requests.exceptions.HTTPError as HTTP_exception:
        print ("Http Error:",HTTP_exception)
    except requests.exceptions.ConnectionError as Connection_exception:
        print ("Error Connecting:",Connection_exception)
    except requests.exceptions.Timeout as Timeout_exception:
        print ("Timeout Error:",Timeout_exception)
    except requests.exceptions.RequestException as Request_exception:
        print ("OOps: Something Else",Request_exception) 
    
df = pd.DataFrame(appended_data, columns=['lead_id','created_at','phone_number', 'original_phone', 'status_code', 'content'])

#once posted we wanted to store the records in the postgresql database to keep a record of what has been sent
df.to_sql(dwh_table , con=engine, if_exists='append', chunksize=100000, index=False)
            
print(f"successfully uploaded:{records} records")
connection.close()
engine.dispose()
