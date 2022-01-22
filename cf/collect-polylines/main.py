import datetime
import json
import logging
import os

import pytz
from google.cloud import storage

METERS_TO_MILES = 0.000621371
RAW_BUCKET = os.environ['RAW_BUCKET']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
OUTPUT_FILENAME = 'polylines.json'
YEAR = 2022
EASTERN_TZ = pytz.timezone('America/New_York')
storage_client = storage.Client()
input_bucket = storage_client.bucket(RAW_BUCKET)
output_bucket = storage_client.bucket(OUTPUT_BUCKET)


def collect_polylines(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'triggered with event {event}')

    activities = []
    total_mi = 0
    activity_dates = set()
    for blob in input_bucket.list_blobs(prefix=f'{YEAR}/'):
        activity = json.loads(blob.download_as_text())
        distance_mi = METERS_TO_MILES * activity['activity']['distance']
        date = activity['activity']['start_date_local'][:10]
        activity_dates.add(date)
        activities.append({
            'id':
            activity['activity']['id'],
            'name':
            activity['activity']['name'],
            'polyline':
            activity['activity']['map']['summary_polyline'],
            'distance_mi':
            distance_mi,
            'date':
            date,
        })
        total_mi += distance_mi

    day_of_year = datetime.datetime.now(tz=EASTERN_TZ).timetuple().tm_yday
    stats_dict = {
        'activities': activities,
        'total_mi': total_mi,
        'day_of_year': day_of_year,
        'days_active': len(activity_dates),
        'last_updated': datetime.datetime.now(tz=pytz.utc).isoformat(),
    }

    out_blob = output_bucket.blob(f'{YEAR}_stats.json')
    out_blob.upload_from_string(json.dumps(stats_dict),
                                content_type='application/json')
    out_blob.make_public()


if __name__ == '__main__':
    collect_polylines(None, None)
