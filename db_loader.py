import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm


def load_data_to_db(data_dir, db_file):
    engine = create_engine(f"sqlite:///{db_file}")

    files_to_process = []
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                files_to_process.append(os.path.join(root, file))

    with engine.begin() as connection:
        for file_path in files_to_process:
            table_name = os.path.splitext(os.path.basename(file_path))[0]

            # Read the number of lines in the CSV file for progress tracking
            num_lines = sum(1 for _ in open(file_path, 'r')) - 1  # Subtracting 1 to exclude header

            # Process the CSV file in chunks. I put 5000 rows as a heuristic rule
            chunksize = 5000
            chunk_iter = pd.read_csv(file_path, low_memory=False, chunksize=chunksize)

            with tqdm(total=num_lines, desc=f"Loading {table_name} into DB", unit="rows") as pbar:
                for i, chunk in enumerate(chunk_iter):
                    if i == 0:
                        chunk.to_sql(table_name, connection, if_exists="replace", index=False)
                    else:
                        chunk.to_sql(table_name, connection, if_exists="append", index=False)

                    # Update the progress bar
                    pbar.update(chunk.shape[0])
