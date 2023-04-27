import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

# Function that loads data from CSV files in the data directory to the SQLite database.
def load_data_to_db(data_dir, db_file):
    """
    Loads CSV data from the data directory into the database.

    Args:
        data_dir (str): The path to the directory containing the CSV files.
        db_file (str): The path to the SQLite database file.

    Returns:
        None. main.py moves on to the transformation.py after this runs successfully.
    """
    # Creates a connection to the SQLite database
    engine = create_engine(f"sqlite:///{db_file}")
    # Logic to find all CSV files in the data directory
    files_to_process = []
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                files_to_process.append(os.path.join(root, file))
    # Logic to process each CSV file and load it to the SQLite database
    with engine.begin() as connection:
        for file_path in files_to_process:
            table_name = os.path.splitext(os.path.basename(file_path))[0]

            # Read the number of rows in the CSV file for progress tracking using tqdm
            num_lines = sum(1 for _ in open(file_path, 'r')) - 1  # Subtract 1 to exclude header row in the csv.

            # Process the CSV file in chunks. I put 5000 rows as a base rule. you can change this number based on memory in your system or desired speed.
            chunksize = 5000
            chunk_iter = pd.read_csv(file_path, low_memory=False, chunksize=chunksize)
            # Load the chunks to the database using a progress bar as a visual reference
            with tqdm(total=num_lines, desc=f"Loading {table_name} into DB", unit="rows") as pbar:
                for i, chunk in enumerate(chunk_iter):
                    if i == 0:
                        chunk.to_sql(table_name, connection, if_exists="replace", index=False)
                    else:
                        chunk.to_sql(table_name, connection, if_exists="append", index=False)

                    # Update the progress bar
                    pbar.update(chunk.shape[0])
