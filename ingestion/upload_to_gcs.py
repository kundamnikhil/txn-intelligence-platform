import os
from google.cloud import storage
from datetime import datetime

def upload_to_gcs(bucket_name, source_file, destination_blob=None):
    client = storage.Client(project=os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform"))
    bucket = client.bucket(bucket_name)

    if destination_blob is None:
        filename = os.path.basename(source_file)
        destination_blob = f"raw/{filename}"

    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} -> gs://{bucket_name}/{destination_blob}")
    return destination_blob

if __name__ == "__main__":
    bucket_name = os.environ.get("GCS_BUCKET_NAME", "txn-intelligence-raw")
    date_str = datetime.utcnow().strftime("%Y_%m_%d")
    source_file = f"/tmp/transactions_{date_str}.csv"
    upload_to_gcs(bucket_name, source_file)
