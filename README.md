# CMS-MS-Finder

A simple project to download, process, and analyze CMS (Centers for Medicare & Medicaid Services) claims data for Multiple Sclerosis (MS) patients.

## Overview

The included scripts perform the following steps:

1. Download CMS claims data (in zip files) from the CMS website(https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DESample01).
2. Extract the data (CSV files) from the zip files.
3. Load the CSV files into a SQLite database.
4. Performs data transformations using SQL on the loaded data.
5. Creates and persists new tables with the transformed data.

## Prerequisites

To run this project, you will need Python 3.6+ installed on your system. Additionally, the following Python libraries are required:

- pandas
- requests
- sqlalchemy
- tqdm

## Setup

To set up the project, follow these steps:

1. Clone this repository.`git clone https://github.com/IrvicRodriguez/CMS-MS-Finder`

2. Create a virtual environment. `python -m venv venv` You can name the virtual environment differently.
3. Activate the virtual environment. On MacOs or Linux use: `source venv/bin/activate` on Windows use: `venv\Scripts\activate`.
4. Install the required packages from the `requirements.txt` file using `pip install -r requirements.txt`.

## Running the script

After setting up the project, you can run the `main.py` script to execute the pipeline using: `python main.py`.

This script will download the necessary data files, process them, and store the results in the SQLite database (`cms_data.db`). Be aware run time is based on local system. For example the scripts takes around 10 to 15 Minutes of runtime (Example times come from Intel 3.6GHz quad-core Mac with 64GB's of RAM).

## File Structure

- `main.py`: The main script that executes the data processing pipeline.
- `config.py`: Contains the specific zip file URLs to download from CMS for the rest of the scripts to work.
- `downloader.py`: Contains the function download_and_extract_data for downloading and extracting data files.
- `db_loader.py`: Contains the function load_data_to_db for loading CSV data into the SQLite database.
- `transformations.py`: Contains the function create_and_persist_transformations for creating and persisting data transformations in the database.

Each file contains comments that explain the purpose of each function and how the code works.
