import json
from google.cloud import secretmanager
import pandas as pd
import os
from sqlalchemy import create_engine, text




### Configuring the TCP connection to the database using secret manager ######################

#This function is used to get the secret from GCP 
def get_secret_details(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    # We get a response string with the secret 
    response = client.access_secret_version(name=secret_name)
    # We modify the payload to create a dictionary with keys
    #  {"host": "database ip", "user": username, "password":"password", "database":"database_name"}
    return json.loads(response.payload.data.decode('utf-8'))


project_id = 'earnest-smoke-371616' #Put the GCP project ID where your database is located
secret_id = 'dbsecret' # Put the name of the secret you have created
version_id = 1 

secret_name = f'projects/{project_id}/secrets/{secret_id}/versions/{version_id}'
secret_details = get_secret_details(secret_name)

print(secret_details)

# We are passing the secret values in to our connection uri 
conn_uri = 'postgresql://{user}:{password}@{host}:{port}/{database}'
conn_uri = conn_uri.format(port=5432, **secret_details)
print(conn_uri)
print(type(conn_uri))


# You will pass this to the SQL python command
engine = create_engine(conn_uri)

#Here is your query

query = '''
    SELECT * FROM information_schema.tables
    WHERE table_catalog = 'itversity_retail_db'
        AND table_schema = 'public'
'''

# Here we are doing some querys 
df = pd.read_sql_query(sql=text(query), con=engine.connect())

print(df)
