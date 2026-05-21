import sys
import os
import warnings
import traceback
import logging
import time
from hdfs_utils import HDFSClient

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'shared_utils'))
sys.path.append(utils_path)

from helpers import load_cfg

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./infra/configurations/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

HDFS_HOST = 'namenode'
HDFS_PORT = 8020
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
BUCKET_NAME_3 = datalake_cfg['bucket_name_3']
###############################################


###############################################
# PySpark
###############################################
def delta_convert(host, port):
    """
        Convert parquet file to delta format
    """
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    builder = SparkSession.builder \
                    .appName("Converting to Delta Lake") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.defaultFS", f"hdfs://{host}:{port}")
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logging.info('Spark session successfully created!')


    # Create bucket 'delta' equivalent in HDFS
    client = HDFSClient(host=host, port=port)
    client.create_directory(f"/{BUCKET_NAME_3}")

    # Convert to delta
    for file in client.list_parquet_files(f"/{BUCKET_NAME_2}/{datalake_cfg['folder_name']}/"):
        path_read = f"hdfs://{host}:{port}" + file
        logging.info(f"Reading parquet file: {path_read}")

        df = spark.read.parquet(path_read)

        # Save to bucket 'delta' 
        logging.info(f"Saving delta file: {file}")

        df_delta = df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(f"hdfs://{host}:{port}/{BUCKET_NAME_3}/{datalake_cfg['folder_name']}")
        
        logging.info("="*50 + "COMPLETELY" + "="*50)
###############################################


###############################################
# Main
###############################################
if __name__ == "__main__":
    delta_convert(HDFS_HOST, HDFS_PORT)
###############################################
