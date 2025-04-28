import os
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import json

# --------- CONFIG ---------
# Azure Config
AZURE_CONNECTION_STRING = "your-azure-blob-connection-string"
CONTAINER_NAME = "your-container-name"

# Kaggle Config
os.environ['KAGGLE_USERNAME'] = "your-kaggle-username"
os.environ['KAGGLE_KEY'] = "your-kaggle-key"

# RapidAPI Config
RAPIDAPI_KEY = "your-rapidapi-key"
RAPIDAPI_HOST = "nba-api-free-data.p.rapidapi.com"

# Temporary local download path
LOCAL_DATA_DIR = "data_temp"
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

# --------- FUNCTIONS ---------

def upload_to_azure(local_path, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {blob_name} to Azure Blob Storage.")

def download_kaggle_dataset(dataset_name, file_name, save_as):
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(dataset_name, file_name, path=LOCAL_DATA_DIR)
    
    full_path = os.path.join(LOCAL_DATA_DIR, save_as)
    os.rename(os.path.join(LOCAL_DATA_DIR, file_name), full_path)
    print(f"Downloaded {save_as} from Kaggle.")
    return full_path

def fetch_rapidapi_data(endpoint, params, headers):
    url = f"https://{RAPIDAPI_HOST}/{endpoint}"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def save_json(data, save_path):
    with open(save_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Saved API response to {save_path}.")

# --------- SCRIPT ---------

def main():
    ### Download Kaggle Datasets
    kaggle_downloads = [
        # NBA Salaries Dataset
        {
            "dataset": "omarsobhy14/nba-players-salaries",
            "file": "NBA_season1718_salary.csv",
            "save_as": "nba_salaries.csv"
        },
        # NBA Basketball Dataset
        {
            "dataset": "wyattowalsh/basketball",
            "file": "Player Per Game Stats.csv",
            "save_as": "player_stats.csv"
        }
    ]

    for item in kaggle_downloads:
        path = download_kaggle_dataset(item["dataset"], item["file"], item["save_as"])
        upload_to_azure(path, item["save_as"])

    ### Fetch from RapidAPI
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }
    
    # Example endpoint: getAllPlayers
    rapidapi_data = fetch_rapidapi_data(endpoint="players", params={"team":"Golden State Warriors"}, headers=headers)
    rapidapi_path = os.path.join(LOCAL_DATA_DIR, "nba_players_api.json")
    save_json(rapidapi_data, rapidapi_path)
    upload_to_azure(rapidapi_path, "nba_players_api.json")

if __name__ == "__main__":
    main()
