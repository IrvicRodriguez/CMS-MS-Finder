import os
import requests
from zipfile import ZipFile
from io import BytesIO
from tqdm import tqdm


def download_and_extract_data(urls):
    for url in tqdm(urls, desc="Downloading zip files"):
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get("content-length", 0))
        zipfile_data = BytesIO()

        for chunk in tqdm(response.iter_content(chunk_size=1024), desc=f"Downloading {os.path.basename(url)}",
                          total=(total_size // 1024) + 1, unit="KB"):
            zipfile_data.write(chunk)

        zipfile_data.seek(0)
        zipfile = ZipFile(zipfile_data)
        zipfile.extractall("data")
