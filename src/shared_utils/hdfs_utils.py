import os
import pyarrow as pa
import pyarrow.fs as fs

class HDFSClient:
    def __init__(self, host='namenode', port=8020, user='root'):
        self.host = host
        self.port = port
        self.user = user
        self.fs = fs.HadoopFileSystem(host=self.host, port=self.port, user=self.user)

    def create_directory(self, path):
        try:
            self.fs.create_dir(path)
            print(f"Directory {path} created successfully")
        except Exception as e:
            print(f"Error creating directory: {e}")

    def upload_file(self, local_path, hdfs_path):
        try:
            from pyarrow.fs import LocalFileSystem
            local_fs = LocalFileSystem()
            fs.copy_files(local_path, hdfs_path, source_filesystem=local_fs, destination_filesystem=self.fs)
            print(f"File uploaded to {hdfs_path} successfully")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def list_parquet_files(self, directory_path):
        try:
            files = self.fs.get_file_info(fs.FileSelector(directory_path, recursive=True))
            return [f.path for f in files if f.path.endswith('.parquet')]
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
