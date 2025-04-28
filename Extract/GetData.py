import os
import requests
from azure.storage.blob import BlobServiceClient
import pandas as pd
import json
import zipfile

# --------- CONFIG ---------
# Azure Config
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datadraft;AccountKey=nqr6YGXwxU+Lb+Yq+J2BBxTlXBH0FZTfgLqba4LdMjfXTaJyMKKIknmXhLjco/Hx7airCXTkEeu5+ASt+cfJKw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "datadraftdata"

# Kaggle Config
os.environ['KAGGLE_USERNAME'] = "matthewashleyz710"
os.environ['KAGGLE_KEY'] = "f62f6ae1a27232a574dacd8a74e10b4e"

# RapidAPI Config
RAPIDAPI_KEY = "71d056184dmshd2688b56e478f6bp12113bjsn1959cd78cb50"
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
    api.dataset_download_files(dataset_name, path=LOCAL_DATA_DIR, unzip=True)
    
    extracted_file_path = os.path.join(LOCAL_DATA_DIR, save_as)
    if not os.path.exists(extracted_file_path):
        raise FileNotFoundError(f"{save_as} not found in the extracted dataset.")
    print(f"Downloaded and extracted {save_as} from Kaggle.")
    return extracted_file_path
def download_and_get_sqlite(dataset_name, expected_sqlite_filename):
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    print(f"Downloading and extracting {dataset_name}...")
    api.dataset_download_files(dataset_name, path=LOCAL_DATA_DIR, unzip=True)

    extracted_file_path = os.path.join(LOCAL_DATA_DIR, expected_sqlite_filename)
    if not os.path.exists(extracted_file_path):
        raise FileNotFoundError(f"Expected {expected_sqlite_filename} not found in {LOCAL_DATA_DIR}.")
    
    print(f"Found SQLite file: {extracted_file_path}")
    return extracted_file_path

def fetch_rapidapi_data(endpoint):
    url = f"https://nba-api-free-data.p.rapidapi.com/{endpoint}"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "nba-api-free-data.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def save_json(data, save_path):
    with open(save_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Saved API response to {save_path}.")

# --------- SCRIPT ---------

def main():
    ## Download Kaggle Datasets
    kaggle_downloads = [
        {
            "dataset": "omarsobhy14/nba-players-salaries",
            "file": "NBA_season1718_salary.csv",
            "save_as": "Nba Player Salaries.csv"
        },
        # {
        #     "dataset": "wyattowalsh/basketball",
        #     "file": "Player Per Game Stats.csv",
        #     "save_as": "nba.sqlite"
        # }
    ]

    for item in kaggle_downloads:
        try:
            path = download_kaggle_dataset(item["dataset"], item["file"], item["save_as"])
            upload_to_azure(path, item["save_as"])
        except Exception as e:
            print(f"Error processing {item['dataset']}: {e}")

    ### Fetch from RapidAPI
    ### Fetch from RapidAPI
    try:
        rapidapi_data = fetch_rapidapi_data("nba-leagues")
        rapidapi_path = os.path.join(LOCAL_DATA_DIR, "nba_leagues_api.json")
        save_json(rapidapi_data, rapidapi_path)
        upload_to_azure(rapidapi_path, "nba_leagues_api.json")
    except Exception as e:
        print(f"Error fetching data from RapidAPI: {e}")


if __name__ == "__main__":
    main()
