import json
import os

from google.cloud import storage

METERS_TO_MILES = 0.000621371
RAW_BUCKET = os.environ['RAW_BUCKET']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
OUTPUT_FILENAME = 'polylines.json'
YEAR = 2022
storage_client = storage.Client()
input_bucket = storage_client.bucket(RAW_BUCKET)
output_bucket = storage_client.bucket(OUTPUT_BUCKET)


def collect_polylines(event, context):
    polylines = []
    total_mi = 0
    for blob in input_bucket.list_blobs(prefix=f'{YEAR}/'):
        activity = json.loads(blob.download_as_text())
        polylines.append(activity['activity']['map']['summary_polyline'])
        total_mi += (METERS_TO_MILES * activity['activity']['distance'])

    stats_dict = {'polylines': polylines, 'total_mi': total_mi}

    out_blob = output_bucket.blob(f'{YEAR}_stats.json')
    out_blob.upload_from_string(json.dumps(stats_dict))
    out_blob.make_public()


if __name__ == '__main__':
    collect_polylines(None, None)
