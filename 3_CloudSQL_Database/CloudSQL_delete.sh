#!/bin/bash

#Run this script to delete your SQL instance

server_name="retail-server"
export PROJECT_ID=$(gcloud config get-value project)

gcloud sql instances delete $server_name --project=$PROJECT_ID