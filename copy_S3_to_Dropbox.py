#!/usr/bin/env python3

import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import dropbox
from dropbox.common import PathRoot
import os
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from dotenv import load_dotenv
import time
import requests
import sys
load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    #endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    #config=Config(signature_version="s3v4")
    #config=Config(signature_version=UNSIGNED)
)
dbx = dropbox.Dropbox(os.getenv("DROPBOX_ACCESS_TOKEN"))
root_namespace_id = dbx.users_get_current_account().root_info.root_namespace_id
dbx = dbx.with_path_root(PathRoot.root(root_namespace_id))

paginator = s3.get_paginator("list_objects_v2")
all_files = []
pages = paginator.paginate(Bucket=os.getenv("S3_BUCKET_NAME"), Prefix=os.getenv("S3_PREFIX"))
for page in tqdm(pages):
    all_files.extend(page.get("Contents"))

df = pd.json_normalize(all_files)
print(df)
print(f"{len(df[df.Size > 0])} files, {round(df.Size.sum() / 1e9, 2)}GB")

def save_key(key):
  try:
    dbx.files_get_metadata(os.getenv("DROPBOX_FOLDER") + key)
    return
  except dropbox.exceptions.ApiError as e:
    pass
  url = s3.generate_presigned_url('get_object', Params={'Bucket': os.getenv("S3_BUCKET_NAME"), 'Key': key})
  start = time.time()
  result = dbx.files_save_url(os.getenv("DROPBOX_FOLDER") + key, url)
  while True:
    check_result = dbx.files_save_url_check_job_status(result.get_async_job_id())
    if check_result.is_in_progress():
      time.sleep(5)
    else:
      if check_result.is_failed():
        print(f"FAILED after {time.time()-start}s", url, check_result)
        print(df[df.Key == key])
      return check_result

results = thread_map(save_key, df.Key, max_workers=10)
failures = [r.is_failed() for r in results if r is not None]
print(f"{sum(failures)} failures / {len(results)} files")
