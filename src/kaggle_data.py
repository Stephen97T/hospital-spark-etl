import os
import shutil

import kagglehub


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
