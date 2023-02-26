from google.cloud import storage
import glob
import os

# Source directory (if your folder is two levels above then use ../../ceres/retail_db)
src_base_dir = 'ceres/retail_db'

#List all the items in the directory folder
items = glob.glob(f'{src_base_dir}/**', recursive=True)

#Print as a list all the elements inside ceres/retail_db folder
print(items)

#Select the second item of the ceres/retail_db folder
#The reason behind that is because the we don't need firt two elements of the list 
#Because they are item[0]='ceres/retail_db' and item[1]='ceres/retail_db/products'
#We need the items of the folder products items[2] = 'ceres/retail_db/products/part-0000', item[3] = 'ceres/retail_db/create_db.sql'  

print(items[2])

# This command verify if the item file exist in the folder -> output: TRUE or FALSE
print(os.path.isfile(items[2]))

# This command apply a filter to get all the files in items using os.path.isfile(), as we saw above the first two
# elements of the 'items' list are folders so we need to apply a filter fo get just the files
files = list(filter(lambda item: os.path.isfile(item), items))
print(files)

# Get the first element of file = files[0] = 'ceres/retail_db/products/part-00000'
file = files[0]
print(file)

# file is 'ceres/retail_db/products/part-00000', split creates a list with
# ['ceres', 'retail_db', 'products','part-0000'] -> we use [1:] to indicate we don't want
# the first two directories we want this: ['products','part-0000'] 
print(file.split('/')[2:])

# We join the list using '/' to get this = 'products/part-0000' 
print('/'.join(file.split('/')[2:]))

#####  Uploading the folders into the cloud storage bucket #####

# Creating a Cloud storage client
gsclient = storage.Client()

# Target Bucket folder to store the files, it doesn't matter if the folder doesn't exists
# It will be created for us 
tgt_base_dir = '1_uploading_data_with_python'


# Bucket name created in the setup.sh file
bucket_name = 'ceres_bucket'

# Get the files we want from the source 
files = filter(lambda item: os.path.isfile(item), items)

# Create a bucket object 
bucket = gsclient.get_bucket(bucket_name)

# Upload the files to the bucket
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

##### In order to delete the bucket you can do this #####

# First we need to delete the files and folders in our bucket
blobs = bucket.list_blobs()
for blob in blobs:
    blob.delete()

# When the bucket is empty then we can delete it
bucket.delete()