import kagglehub
import shutil
import os
import glob
from pathlib import Path

def setup_master_data():
    KAGGLE_DATASET = "aramacus/electricity-demand-in-victoria-australia"
    TARGET_DIR_NAME = "master"
    TARGET_FILE_NAME = "electricity.csv"
    
    PROJECT_ROOT = Path(__file__).parent.absolute()
    DATALAKE_DIR = PROJECT_ROOT / "datalake"
    TARGET_DIR = DATALAKE_DIR / TARGET_DIR_NAME
    TARGET_FILE = TARGET_DIR / TARGET_FILE_NAME

    print("download data from Kaggle...")
    try:
        cache_path = kagglehub.dataset_download(KAGGLE_DATASET)
        print(f"complete downloaded(Cache path: {cache_path})")
    except Exception as e:
        print(f"error : {e}")
        return

    print(f"extracting : {TARGET_DIR}")
    os.makedirs(TARGET_DIR, exist_ok=True)

    csv_files = glob.glob(os.path.join(cache_path, "*.csv"))

    if not csv_files:
        print(f"no such file .csv in this dataset ({cache_path})")
        return

    source_file = csv_files[0]
    
    print(f"import data \nfrom: {source_file}\nto: {TARGET_FILE}")
    
    try:
        shutil.copy2(source_file, TARGET_FILE)
        print("data is ready")
    except Exception as e:
        print(f"error occur: {e}")

if __name__ == "__main__":
    setup_master_data()