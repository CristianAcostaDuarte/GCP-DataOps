#!/bin/bash

# If you don´t have a Makefile that create the venv automatically then use the following
# commands to create a venv automatically -> to execute the file use: ". ./setup.sh"

# create a new virtual environment
python -m venv myenv

# activate the virtual environment
source myenv/bin/activate

# install required packages
pip install -r requirements.txt

# set the environment variable for the virtual environment
export VIRTUAL_ENV=myenv

# append the export statement to the bashrc file
echo "export VIRTUAL_ENV=myenv" >> ~/.bashrc

# Creating the folder for the project
# Set the name of the folder to be created
folder_name="ceres"

# Check if the folder exists
if [ -d "$folder_name" ]; then
  echo "The folder already exists."
else
  # Create the folder if it does not exist
  mkdir "$folder_name"
  echo "The folder has been created."
fi

# Copy the data form the repository to the project folder

repo_url="https://github.com/dgadiraju/data.git"
cwd=$(pwd) #Current directory
clone_path="$cwd/$folder_name"
git clone "$repo_url" "$clone_path"


# Create the bucket (remember to first configure the credentials with cloud init -> then cloud auth)
gsutil mb gs://ceres_bucket
#gsutil cp -r gs://ceres_bucket/data/retail_db