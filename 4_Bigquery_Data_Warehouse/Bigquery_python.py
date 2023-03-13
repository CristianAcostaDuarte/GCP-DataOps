from google.cloud import storage
import glob
import os

##################### First upload data from ceres repository to GCS bucket####################3
src_base_dir = 'ceres/retail_db'

# The files inside the retail_db folder
items = glob.glob(f'{src_base_dir}/**', recursive=True)
files = list(filter(lambda item: os.path.isfile(item), items))

gsclient = storage.Client()

# Bucket name
bucket_name = 'ceres_bucket'

# Folder inside ceres bucket -> if the folder doesn't exists then it will be created
tgt_base_dir = 'retail_db'

files = filter(lambda item: os.path.isfile(item), items)

# Bucket object creation
bucket = gsclient.get_bucket(bucket_name)

for file in files:
    print(f'Uploading file {file}')
    blob_suffix = '/'.join(file.split('/')[2:])
    blob_name = f'{tgt_base_dir}/{blob_suffix}'
    blob = bucket.blob(blob_name) # To upload we first create a blob object (folder/files) and then 
    # We upload the blobs into our bucket
    blob.upload_from_filename(file)

# Verify the data was uploaded
gsclient.list_blobs(
    bucket_name,
    prefix= tgt_base_dir 
)

blobs = list(gsclient.list_blobs(
    bucket_name,
    prefix= tgt_base_dir
))

# Here you will find all the files and folders inside your bucket
print(blobs)

##################### Upload data form cloud storage to Bogquery table ####################3


from google.cloud import bigquery

# Open a Bigquery client

client = bigquery.Client()

# Select the project ID where your table is located

project_id = 'earnest-smoke-371616'
dataset = 'retail'
table_1 = 'orders'
table_2 = 'order_items'
table_3 = 'products'

# Bigquery 'orders' table string and the schema of the table
bq_table_1 = f"{project_id}.{dataset}.{table_1}"

job_config_1 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("order_id", "INTEGER"),
        bigquery.SchemaField("order_date", "TIMESTAMP"),
        bigquery.SchemaField("order_customer_id", "INTEGER"),
        bigquery.SchemaField("order_status", "STRING")
    ],
)

# Bigquery 'order_items' table string and schema of the table
bq_table_2 = f"{project_id}.{dataset}.{table_2}"

job_config_2 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("order_item_id", "INTEGER"),
        bigquery.SchemaField("order_item_order_id", "INTEGER"),
        bigquery.SchemaField("order_item_product_id", "INTEGER"),
        bigquery.SchemaField("order_item_quantity", "INTEGER"),
        bigquery.SchemaField("order_item_subtotal", "FLOAT"),
        bigquery.SchemaField("order_item_product_price", "FLOAT")
    ],
)

# Bigquery 'products' table string and schema of the table
bq_table_3 = f"{project_id}.{dataset}.{table_3}"

job_config_3 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("product_id", "INTEGER"),
        bigquery.SchemaField("product_category_id", "INTEGER"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("product_description", "STRING"),
        bigquery.SchemaField("product_price", "FLOAT"),
        bigquery.SchemaField("product_image", "STRING")
    ],
)

# Cloud storage bucket
uri = "gs://ceres_bucket/retail_db/orders/part-00000"


# Loading the data from table 1 -> orders table
load_job_1 = client.load_table_from_uri(
    uri, bq_table_1, job_config=job_config_1
)  # Make an API request.

print(load_job_1.result())  # Wait for the job to complete.

table = client.get_table(bq_table_1)
print("Loaded {} rows to table {}".format(table.num_rows, bq_table_1))


# Perform a Query

QUERY = (
    f'''SELECT * FROM `{project_id}.{dataset}.{table_1}` LIMIT 10'''
)
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.values())

##################### The last part is using pandas to manage Bigquery data ####################3

import pandas as pd

query = f'''
    SELECT order_status, count(*) AS order_count
    FROM `{project_id}.{dataset}.{table_1}`
    GROUP BY 1
    ORDER BY 2 DESC
'''
#We can use the pandas-gbq for create a dataframe of a table in Bigquery

df = pd.read_gbq(query, project_id=project_id)
print(df)


##### In order to delete the bucket you can do this #####

# First we need to delete the files and folders in our bucket
#blobs = bucket.list_blobs()
#for blob in blobs:
    #blob.delete()

# When the bucket is empty then we can delete it
#bucket.delete()