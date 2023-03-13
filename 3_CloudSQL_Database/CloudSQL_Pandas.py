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


project_id = '' #Put the GCP project ID where your database is located
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

query = 'SELECT * FROM orders LIMIT 10'

# Here we are doing some querys 
df = pd.read_sql_query(sql=text(query), con=engine.connect())

print(df)


# columns = ['order_id', 'order_date', 'order_customer_id', 'order_status']
# schemas = json.load(open('../../data/retail_db/schemas.json'))
# print(sorted(schemas['orders'], key=lambda col: col['column_position']))

# columns = [col['column_name'] for col in sorted(schemas['orders'], key=lambda col: col['column_position'])]
# print(columns)

# orders = pd.read_csv('ceres/retail_db/orders/part-00000', names=columns)

# daily_status_count = orders. \
#     groupby(['order_date', 'order_status'])['order_id']. \
#     agg(order_count='count'). \
#     reset_index()

# daily_status_count.to_sql(
#     'daily_status_count',
#     conn_uri,
#     if_exists='replace',
#     index=False
# )

# dsc = pd.read_sql(
#     'daily_status_count',
#     conn_uri
# )

# dsc = pd.read_sql(
#     '''
#         SELECT order_status, sum(order_count) AS order_count FROM daily_status_count
#         GROUP BY 1
#         ORDER BY 2 DESC
#     ''',
#     conn_uri
# )
