import os
import requests
from zipfile import ZipFile
from io import BytesIO
from tqdm import tqdm

# Function to download and extract zip files from URLs
def download_and_extract_data(urls):
    """
    Downloads and extracts data from the given URLs in config.py.

    Args:
        urls (list): A list of URLs containing the data files to be downloaded and extracted.

    Returns:
        None. main.py will move on to load data once this function finishes.
    """
    for url in tqdm(urls, desc="Downloading zip files"):
        # Make a request to download a zip file
        response = requests.get(url, stream=True)
        # Get the total file size from response headers. we use this to calculate how much more data we have to process
        total_size = int(response.headers.get("content-length", 0))
        # Create a BytesIO object to store the zip file data in memory without saving the zip file locally.
        zipfile_data = BytesIO()
        # Download the zip file in chunks. I use 1024 in my local system. you can play with chunk_size based on your system memory or desired performance
        for chunk in tqdm(response.iter_content(chunk_size=1024), desc=f"Downloading {os.path.basename(url)}",
                          total=(total_size // 1024) + 1, unit="KB"):
            zipfile_data.write(chunk)
        # Start at the beginning of the BytesIO object
        zipfile_data.seek(0)
        # Read the zip file
        zipfile = ZipFile(zipfile_data)
        # Extract the zip file to the 'data' directory
        zipfile.extractall("data")
