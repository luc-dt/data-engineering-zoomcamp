import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "data-raw-lake-nguyen-486008")

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client(project="de-zoomcamp-2026-486008")
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service):
    for i in range(1, 13):

        # set the month part of the file_name string
        month = str(i).zfill(2)
        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        request_url = f"{init_url}{service}/{file_name}"
        
        print(f"Processing: {file_name}")

        # download it using requests via a pandas df
        r = requests.get(request_url)
        if r.status_code == 200:
            with open(file_name, "wb") as f:
                f.write(r.content)
            print(f"Local: {file_name} downloaded.")

            # Upload to GCS
            upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
            print(f"GCS: {service}/{file_name} uploaded.")
        else:
            print(f"File not found on GitHub: {file_name}")


web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')

web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')

# web_to_gcs('2019', 'fhv')

