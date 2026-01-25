import os
import shutil

import kagglehub
import pandas as pd


def download_and_move_data() -> None:
    # 1. Download latest version (returns the cache path)
    print("Downloading dataset from Kaggle...")
    cache_path = kagglehub.dataset_download("t0ut0u/hospital-excel-dataset")

    # 2. Define your project's local data directory
    local_data_dir = "data"

    # 3. Create the directory if it doesn't exist
    if not os.path.exists(local_data_dir):
        os.makedirs(local_data_dir)
        print(f"Created directory: {local_data_dir}")

    # 4. Move files from cache to local data folder
    # We iterate through files in the cache to avoid moving the folder itself
    for filename in os.listdir(cache_path):
        source = os.path.join(cache_path, filename)
        destination = os.path.join(local_data_dir, filename)

        # Use shutil.copy or shutil.move (copy is safer for debugging)
        shutil.copy(source, destination)
        print(f"Copied {filename} to {local_data_dir}/")

    print("\n✅ Dataset ready in project folder.")


def prepare_hospital_data(excel_path: str = "data/hospital-dataset.xlsx") -> str:
    """Converts the Kaggle Excel file to CSV for Spark optimization.
    Returns the path to the CSV file.
    """
    csv_path = excel_path.replace(".xlsx", ".csv")

    if os.path.exists(csv_path):
        print(f"--- Optimized CSV already exists at {csv_path} ---")
        return csv_path

    if not os.path.exists(excel_path):
        raise FileNotFoundError(
            f"Could not find {excel_path}. Did the Kaggle download finish?"
        )

    print(f"--- Converting {excel_path} to CSV for Spark optimization ---")

    # Use pandas to bridge the gap once
    df = pd.read_excel(excel_path)
    df.to_csv(csv_path, index=False)

    print(f"--- Success: {csv_path} created ---")
    return csv_path
