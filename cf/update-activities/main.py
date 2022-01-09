import datetime
import json
import os
import time
from typing import Dict, Tuple

import dateutil.parser
import firebase_admin
import requests
from firebase_admin import credentials, firestore
from google.cloud import secretmanager, storage

GCS_BUCKET = os.environ['GCS_BUCKET']
GCP_PROJECT = os.environ['GCP_PROJECT']
CLIENT_ID_SECRET_KEY = 'strava-client-id'
CLIENT_SECRET_SECRET_KEY = 'strava-client-secret'
REFRESH_TOKEN_KEY = 'strava-refresh-token'
LAST_SYNC_FILE = 'last-sync.txt'
QUEUE_KEY = 'activity-update-queue'

secret_client = secretmanager.SecretManagerServiceClient()
storage_client = storage.Client()
storage_bucket = storage_client.bucket(GCS_BUCKET)
firebase_admin.initialize_app(credentials.ApplicationDefault())
db = firestore.client()


def _get_secret(name: str) -> Tuple[str, str]:
    key = f'projects/{GCP_PROJECT}/secrets/{name}/versions/latest'
    secret = secret_client.access_secret_version(name=key)
    return secret.payload.data.decode('UTF-8'), secret.name


def _get_strava_access_token() -> str:
    client_id, _ = _get_secret(CLIENT_ID_SECRET_KEY)
    client_secret, _ = _get_secret(CLIENT_SECRET_SECRET_KEY)
    refresh_token, refresh_token_name = _get_secret(REFRESH_TOKEN_KEY)
    resp = requests.post('https://www.strava.com/api/v3/oauth/token',
                         data={
                             'client_id': client_id,
                             'client_secret': client_secret,
                             'refresh_token': refresh_token,
                             'grant_type': 'refresh_token'
                         }).json()
    new_refresh_token = resp['refresh_token']

    if new_refresh_token != refresh_token:
        secret_client.add_secret_version(
            request={
                'parent':
                f'projects/{GCP_PROJECT}/secrets/{REFRESH_TOKEN_KEY}',
                'payload': {
                    'data': new_refresh_token.encode()
                }
            })
        secret_client.destroy_secret_version(name=refresh_token_name)

    return resp['access_token']


def _try_rate_limit_request(url: str, headers: Dict[str, str]):
    while True:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            print(f'rate limited request for {url}, waiting 60 seconds')
            time.sleep(60)
        else:
            # TODO: this should also catch 401s and refresh the access token
            raise Exception(
                f'unexpected error in response for url: {url}: {resp}')


def _update_activity_queue(access_token):
    queue = {d.id: d.to_dict() for d in db.collection(QUEUE_KEY).stream()}

    current_time = int(
        datetime.datetime.now(datetime.timezone.utc).timestamp())

    last_sync_time = 0
    last_sync_blob = storage_bucket.blob(LAST_SYNC_FILE)
    if last_sync_blob.exists():
        last_sync_time = int(last_sync_blob.download_as_text())

    headers = {'Authorization': f'Bearer {access_token}'}

    # Rewind a few hours in case an activity was uploaded that started before the previous sync time but finished after.
    oldest_activity_time = last_sync_time - (60 * 60 * 6)
    print(
        f'fetching activities between {oldest_activity_time} and {current_time}'
    )

    page = 1
    all_activities = []
    while True:
        print(f'fetching page {page}')
        activities = _try_rate_limit_request(
            f'https://www.strava.com/api/v3/athlete/activities?before={current_time}&after={oldest_activity_time}&page={page}',
            headers=headers)

        if not activities:
            print(f'done after {page - 1} pages')
            break

        # TODO(craigatron): this excludes manual activities (e.g. dreadmill)
        all_activities.extend(
            [a for a in activities if a['type'] == 'Run' and not a['manual']])
        page += 1

    print(f'found {len(all_activities)} activities')

    if all_activities:
        for activity in all_activities:
            activity_id = str(activity['id'])
            if activity_id not in queue:
                print(f'adding activity {activity_id}')
                db.collection(QUEUE_KEY).document(activity_id).set(activity)
            else:
                print(f'skipping existing activity {activity_id}')
    else:
        print('no new activities found')

    print('setting new last sync time')
    last_sync_blob.upload_from_string(str(current_time))


def _download_activities(access_token):
    headers = {'Authorization': f'Bearer {access_token}'}
    for activity in db.collection(QUEUE_KEY).stream():
        activity_dict = activity.to_dict()

        if activity_dict['manual']:
            print(f'skipping manual activity {activity_dict["id"]}')
            activity.reference.delete()
            continue

        # TODO(craigatron): can we get an upload date or something?  if you upload an old activity this will skip it
        activity_start_date = dateutil.parser.isoparse(
            activity_dict['start_date']).date()

        filename = f'{activity_start_date.strftime("%Y/%m")}/{activity_dict["id"]}.json'
        blob = storage_bucket.blob(filename)
        if blob.exists():
            print(f'found existing file for {filename}, skipping')
            activity.reference.delete()
            continue

        stream = _try_rate_limit_request(
            f'https://www.strava.com/api/v3/activities/{activity_dict["id"]}/streams?keys=time,latlng,distance,altitude,heartrate,cadence',
            headers=headers)

        data = {'activity': activity_dict, 'streams': stream}
        print(f'uploading to {filename}')
        blob = storage_bucket.blob(filename)
        blob.upload_from_string(json.dumps(data),
                                content_type='application/json')

        activity.reference.delete()


def strava_sync(event, context):
    access_token = _get_strava_access_token()

    _update_activity_queue(access_token)
    _download_activities(access_token)


if __name__ == '__main__':
    strava_sync(None, None)
