#!/bin/bash

# Installing postgresql client
sudo apt-get update
sudo apt-get install postgresql-client

# We are going to create the database using the gcloud sql command //
# We will create a postgre database server in a db-custom-1-3840 instance 
# the specifications of the machine (4GB RAM, 1VCP, and 128Gb of persistent storage)

server_name="retail-server"
database_version="POSTGRES_14"
machine_type="db-custom-1-3840" ## We need to use a custom machine to create a PostgreSQL server instance
region="us-east1"

gcloud sql instances create $server_name --database-version=$database_version  --tier=$machine_type --region=$region

# Create a database in the previous instance -> in order to connect to our instance
# We have to first whitelist the ip from the device we are going to make the querys
# Go to google cloud and configure that, another option is to use SQL proxy //
# With SQL proxy you don't have to whitelist any IP.

database_name="products-db"

gcloud sql databases create $database_name --instance=$server_name

#------------------------  Connecting to the database using SQL proxy     ---------------------------------------------

# Download Cloud SQL Auth proxy:

curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.0.0/cloud-sql-proxy.linux.amd64

# Make it executable

chmod +x cloud-sql-proxy

# Stablish a TCP connection, here we need to specify your localhost 
# We also need to specfy the port your localhost will use to connect to the database
# in these case i recommend use the same port in your localhost as the port in the database, for example:
# MySQL:3306 PostgreSQL:5432
# The instance connection name basically is something that you need to provide to connect to 
# The instance usually is configured as follows "project_id:instance_zone:instance_name"
local_ip="127.0.0.1" # -> this is the localhost , Don't put your real IP here
local_port="5432" #Put the port that you will use in your local host to map to the port 5432 on postgreSQL

# Get the project ID
export PROJECT_ID=$(gcloud config get-value project)

# Create the string for instance connection

instance_connection="$PROJECT_ID:$region:$server_name"

# A more efficent way to perform the previous thing is using this, basically we run the command to describ
# our instance retail_db then we use sed to filter connectionName of the instance

instance_connection="$(gcloud sql instances describe $server_name | sed -n 's/connectionName\s*:\s*\(.*\)/\1/p' | xargs )"


# Stablish the connection -> you have to keep open the terminal of this connection
# Access to the postgre database using another terminal
./cloud-sql-proxy --address $local_ip --port $local_port $instance_connection

# This is the command to access to a postgres sql database -> in order to stablish a connection you need the proxy connection active, in order to connect to the database
# go to the cloud console and give a password to the base user in postgresql the first user is "postgres"
# -> psql "host=127.0.0.1 port=5432 sslmode=disable dbname=products-db user=postgres" # at the moment of database creation, postgres creates an user called postgres
# -> mysql -u root -p --host 127.0.0.1 --port 3306
# -> sqlcmd -S 127.0.0.1 -U USER_NAME
