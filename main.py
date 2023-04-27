from config import DATA_URLS, DB_FILE
from downloader import download_and_extract_data
from db_loader import load_data_to_db
from transformations import create_and_persist_transformations


def run_pipeline():
    download_and_extract_data(DATA_URLS)
    load_data_to_db("data", DB_FILE)
    create_and_persist_transformations(DB_FILE)


if __name__ == "__main__":
    run_pipeline()
