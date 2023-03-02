import glob
import os
import json
import pandas as pd
from google.cloud import storage


# Get the items (recursively) inside the directory where your local data is located
# return a glob object which is a iterable, then we use a filter which accepts a lambda function
# that iterate into the glob object which contains paths with folders, directories and files
# the filter lambda function must returns true or false, in this case in every iteration
# inside the glob object we verify that the object is a file and that the file ends with 'part-0000'

def get_file_names(src_base_dir):
    items = glob.glob(f'{src_base_dir}/**', recursive=True)
    print(list(items[:5]))
    return list(filter(lambda item: os.path.isfile(item) and item.endswith('part-00000'), items))

# After running '. ./2_setup.sh' a ceres folder will be created in the actual folder and the data will
# download from the githib repository in the ceres folder 

src_base_dir = 'ceres/retail_db'

#Print the names of the files we want from our data
print(get_file_names(src_base_dir))

# When you are working with data is important to have the schema of the data
# in json format, in this example the json format for the data inside the
# retail_db folder (remember that a schema is basically a database -> a collection
# of tables with their own columns, etc..)
json_base_dir = 'ceres/retail_db/schemas.json'
schemas = json.load(open(json_base_dir))
print(schemas)

#In this case we want to print the schema (basically a table) called "orders"
# Inside this table we will find that this table has 4 columns
print(schemas['orders'])

#If we want to sort the 'orders' table by the column position
print(sorted(schemas['orders'], key=lambda col: col['column_position']))

#We can save that schema of the 'orders' table in a variable
ds_schema = sorted(schemas['orders'], key=lambda col: col['column_position'])

# Now we can go inside that json file to get the column names of the 'orders' table
print([col['column_name'] for col in ds_schema])

# Now with the previous step combined we can get the column names of a table inside a schema json file
# schemas_file is the file that contains the tables and columns -> you need to specify a local path of where the schema.json file is located
# ds_name is the name of the table that you want to get the columns -> for example in the previous case we used 'orders' table

def get_column_names(schemas_file, ds_name):
    schemas = json.load(open(schemas_file))
    ds_schema = sorted(schemas[ds_name], key=lambda col: col['column_position'])
    columns = [col['column_name'] for col in ds_schema]
    return columns


json_base_dir = 'ceres/retail_db/schemas.json'
table_name = 'orders'
print(get_column_names(json_base_dir , table_name))

# Here we have some of the tables that are described in the schema.json file
# In order to get the column names of each of these tables we can create a list
# with the tables names and iterate over the get_column_names function

for ds in [
    'departments', 'categories', 'products',
    'customers', 'orders', 'order_items'
]:
    column_names = get_column_names('ceres/retail_db/schemas.json', ds)
    print(f'''columns for {ds} are {','.join(column_names)}''')


# Now we are going to use Numpy to upload our data

src_base_dir = 'ceres/retail_db' #Your data directory
schemas_file = 'ceres/retail_db/schemas.json' #Your schema.json path
bucket = 'ceres_bucket' #Your bucket name 
files = get_file_names(src_base_dir) # This will return a list with all the files of your data directory
file = files[0]
print(file)

# if we only want the folder and the file
print('/'.join(file.split('/')[-2:]))

# In this example the folders inside 'retail_db' are the names of tables
# so in order to get that table names we select the second last element,
# then we introduce that name into the get_columns function

ds_name = file.split('/')[-2]
print(ds_name)

columns = get_column_names(schemas_file, ds_name)
print(columns)

############### Now wrap up this together at the same time ##############
#We want to upload parwut files to GCS using numpy

src_base_dir = 'ceres/retail_db' # Directory were our data is located
tgt_base_dir = 'retail_db_parquet' # Directory name in our bucket where we want our data to be located, if the directory doesn' exists then it will be created
schemas_file = 'ceres/retail_db/schemas.json' #This is the path where our schema.json file is located
bucket = 'ceres_bucket' #This is the name of our bucket
files = get_file_names(src_base_dir) #This will return a list with all the files (after a filter ) inside our data directory 

#We will upload file by file
for file in files:
    print(f'Uploading file {file}')
    blob_suffix = '/'.join(file.split('/')[-2:]) #This will be used to create the path for the file in GCS an example output : customer/file-8000
    ds_name = file.split('/')[-2] # This is used to get the name of the tables , taking the previous example the table will be 'customer'
    blob_name = f'gs://{bucket}/{tgt_base_dir}/{blob_suffix}.snappy.parquet' #Here we are creating the path in GCS where we want to upload the file, taking the previous example : gs://ceres/retail_db_parquet/customer/file-8000.snappy.parquet
    columns = get_column_names(schemas_file, ds_name) # This will return the column names of the table specified in ds (the table must be in the schema.json file)
    df = pd.read_csv(file, names=columns) # Here we create a dataframe that reads a file (which is a glob object) and then we pass as second argument the columns we want to retrieve 
    df.to_parquet(blob_name, index=False) # Here we upload the data to our bucket in parquet format

#Get the data from cloud storage to see if the data was uploaded
pd.read_parquet('gs://ceres_bucket/retail_db_parquet/orders/part-00000.snappy.parquet')

#A better way to verify that is to listing the total of objects in the data directory and compare them with the ones in cloud storage

# For the local directory

for ds in [
    'departments', 'categories', 'products',
    'customers', 'orders', 'order_items'
]:
    df = pd.read_csv(f'ceres/retail_db/{ds}/part-00000', header=None)
    print(f'''Shape of {ds} in local files system is {df.shape}''')

# For the bucket
for ds in [
    'departments', 'categories', 'products',
    'customers', 'orders', 'order_items'
]:
    df = pd.read_parquet(f'gs://{bucket}/{tgt_base_dir}/{ds}/part-00000.snappy.parquet')
    print(f'''Shape of {ds} in gcs is {df.shape}''')



##### In order to delete the bucket you can do this #####
gsclient = storage.Client()
bucket_name = 'ceres_bucket'
bucket = gsclient.get_bucket(bucket_name)

# First we need to delete the files and folders in our bucket
blobs = bucket.list_blobs()
for blob in blobs:
    blob.delete()

# When the bucket is empty then we can delete it
bucket.delete()