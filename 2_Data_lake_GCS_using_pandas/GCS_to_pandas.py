import pandas as pd

# Select the shcema of the csv file
columns = ['order_id', 'order_date', 'order_customer_id', 'order_status']

# The path of the csv file tou want to download
bucket_path = 'gs://ceres_bucket/1_uploading_data_with_python/orders/part-00000'

# Create a dataframe with the csv file located in the bucket
df = pd.read_csv(bucket_path, names=columns)
print(df)

# Pass data to parquet -> is a efficent way to dota compression on columnar tables
bucket_path = 'gs://ceres_bucket/1_uploading_data_with_python/orders/part-00000.snappy.parquet'
df.to_parquet(
    bucket_path,
    index=False
)

df = pd.read_parquet(bucket_path)