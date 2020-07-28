from google.cloud import storage
import json

def get_products():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ingka-rrm-ugc-data')
    input_blob = bucket.blob('testing/ugc_api.json').download_as_string()
    return json.loads(input_blob)