"""
Main script for uploading exaspim-to-template-to-CCF registration results.
"""
import json
import os
from typing import List, Optional
import argparse
import glob
import s3fs
from urllib.parse import urlparse

from pathlib import Path


def upload_alignment_data(
    s3_path: str,
    folder_to_upload: str,
) -> str:
    """
    generate output meta data, processing.json
    Copies results to the destination bucket to make it available
    to scientists as soon as possible.

    Parameters
    ----------
    s3_path: str
        New dataset name where the data will
        be copied following the aind conventions
        e.g., s3://{bucket_path}/{new_dataset_name}

    folder_to_upload: str
        Results folder path in Code Ocean

    Returns
    -------
    Tuple[str, str]
        The first position is the path where the dataset
        was moved. e.g., s3://{bucket_path}/{new_dataset_name}
        It includes the "s3://" prefix. 
        e.g., s3://{bucket_path}/{new_dataset_name}/{output_prediction}
    """

    #------------------------------------#
    # upload alignment results to s3
    #------------------------------------#
    # s3_path = f"s3://{bucket_path}/{new_dataset_name}"
    print(f"upload files to path {s3_path}")

    fs = s3fs.S3FileSystem()
    url = urlparse(s3_path)
    print(f"url: {url}")

    if url.scheme != "s3":
        raise NotImplementedError("Only s3 output_uri is supported, not {url.scheme}")
    
    print(f"uploading {folder_to_upload}")
    fs.put(
        folder_to_upload, url.netloc + url.path.rstrip("/") + "/", recursive=True, maxdepth=10
    ) 


def get_root_s3_prefix(s3_uri, levels_up=1):
    # Remove 's3://' and split path
    scheme, bucket_and_key = s3_uri.split('://', 1)
    bucket, *key_parts = bucket_and_key.split('/')
  
    # Go `levels_up` directories up from the current file path
    base_key = '/'.join(key_parts[:levels_up])
    return f's3://{bucket}/{base_key}/'

def main() -> None:
    """
    Main function to run the CCF registration pipeline.
    
    This function orchestrates the entire registration process:
    1. Loads configuration from processing manifest
    2. Sets up output directories and logging
    3. Performs registration at the specified resolution level
    4. Optionally applies transforms to 10um resolution
    5. Generates processing metadata
    """
    DATA_FOLDER = os.path.abspath("../data")
    RESULTS_FOLDER = os.path.abspath("../results")
    processing_manifest_file = os.path.abspath(glob.glob(f"{DATA_FOLDER}/*.json")[0])
    try:
        with open(processing_manifest_file, 'r') as f:
            dataset_config = json.load(f)
    except FileNotFoundError:
        print(f"Error: {processing_manifest_file} not found.")
        return

    print(f"processing_manifest_file: {processing_manifest_file}")

    dataset_path = str(dataset_config["zarr_multiscale"]["input_uri"])
    s3_reg_path = get_root_s3_prefix(dataset_path)
    # if "aind-open-data" in s3_reg_path:
    #     s3_reg_path = s3_reg_path.replace("aind-open-data", "aind-scratch-data")
    print(f"Upload reg to {s3_reg_path}")

    outprefix_reg = f"{DATA_FOLDER}/ccf_alignment/"
    print(f"folder_to_upload: {outprefix_reg}")
    
    upload_alignment_data(
        s3_reg_path,
        outprefix_reg,
    )

    outprefix_reg = f"{DATA_FOLDER}/soma_detection/"
    print(f"folder_to_upload: {outprefix_reg}")
    upload_alignment_data(
        s3_reg_path,
        outprefix_reg,
    )

    filename = f"{RESULTS_FOLDER}/finished_registration.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(s3_reg_path)
        
if __name__ == "__main__":
    main()