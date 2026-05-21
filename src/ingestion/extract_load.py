import sys
import os
from glob import glob

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'shared_utils'))
sys.path.append(utils_path)

from helpers import load_cfg
from hdfs_utils import HDFSClient
###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./infra/configurations/datalake.yaml"
YEARS = ["2020", "2021", "2022", "2023"]
###############################################


###############################################
# Main
###############################################
def extract_load(host, port):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = HDFSClient(host=host, port=port)

    # Create root raw directory
    client.create_directory(f"/{datalake_cfg['bucket_name_1']}")

    for year in YEARS:
        # Upload files
        all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))

        for fp in all_fps:
            print(f"Uploading {fp}")
            hdfs_path = f"/{datalake_cfg['bucket_name_1']}/{datalake_cfg['folder_name']}/{os.path.basename(fp)}"
            client.upload_file(fp, hdfs_path)


if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    HDFS_HOST = 'namenode'
    HDFS_PORT = 8020

    extract_load(HDFS_HOST, HDFS_PORT)