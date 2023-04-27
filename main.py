from config import DATA_URLS, DB_FILE
from downloader import download_and_extract_data
from db_loader import load_data_to_db
from transformations import create_and_persist_transformations

# This function runs the entire pipeline
def run_pipeline():
    """
    Orchestrates the pipeline by calling functions to download data, load it into a SQLite database, and applies transformations.

    Returns:
        Nothing as the output... I should maybe add a sucessfull message at the end.
    """
    # Download and extract the data that is pulled from URL's in config.py into the data folder
    download_and_extract_data(DATA_URLS)
    # Load the data folder files into the SQLite database
    load_data_to_db("data", DB_FILE)
    # Create and persist new tables from SQL in transformations.py
    create_and_persist_transformations(DB_FILE)

# Run the pipeline if this script is the main entry point. example:$ python main.py
if __name__ == "__main__":
    run_pipeline()
