from google.cloud import storage
import json

def get_products():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('<bucket_name>')
    input_blob = bucket.blob('<path_of_test_date_input_file>').download_as_string()
    return json.loads(input_blob)