import os
import subprocess
from tqdm import tqdm
from datetime import datetime


from data.hdm import PlatformApi

dev_platform = PlatformApi(platform='dev')
stable_platform = PlatformApi(platform='stable')


def get_session_id(session_data) -> str or None:
    if session_data['items']:
        return session_data['items'][0]['id']
    else:
        return None

    
def service_file_connection(platform='dev') -> None:
    key_file = './service/gcloud_api_key_roadly_dev.json' if platform == 'dev' else './service/gcloud_api_key_roadly_stable.json'
    os.system(f'gcloud auth activate-service-account --key-file={key_file}')


def get_files_in_gcloud_bucket(url: str) -> list[str]:
    try:
        files = subprocess.check_output(['gsutil', 'ls', url], text=True)
        files = files.strip().split('\n')
        # Strip the base URL from each file path
        files = [file.replace(url, '').strip('/') for file in files if file.replace(url, '').strip('/')]
    except subprocess.CalledProcessError:
        print(f"Failed to list files in {url}")
        files = []
    return files

def get_session_data(session_data: dict, keys: list) -> dict:
    return {key: session_data.get(key) for key in keys}

def prepare_data_for_session_collection(session_data: dict) -> dict:
    data_to_collection = get_session_data(session_data, ['platform', 'folder_prefix', 'folder_name', 'has_pipelines', 'track_line'])
    data_to_collection['pipelines'] = [pipeline.get('uuid') for pipeline in session_data.get('pipelines', [])]
    data_to_collection['place_data'] = []
    data_to_collection['session_id'] = session_data['id']

    geocoding = session_data.get('geocoding', {})
    if geocoding:
        features = geocoding.get('features', [])
        if features:
            for feature in features:
                if isinstance(feature, dict):
                    place_type = feature.get('place_type')[0]
                    place_name = feature.get('place_name')
                    if not isinstance(place_type, list) and not isinstance(place_name, list):
                        data_to_collection['place_data'].append({place_type: place_name})

    return data_to_collection

def prepare_data_for_recslam_collection(session_data: dict) -> dict:
    data_to_collection = get_session_data(session_data, ['platform', 'user'])
    data_to_collection['session_id'] = session_data['id']
    data_to_collection['meta'] = get_session_data(session_data, ['is_broken', 'is_my', 'is_invalid', 'device', 'os', 'recslam_build', 'recslam_version', 'create_ts', 'distance_calc_raw', 'provider'])
    data_to_collection['files'] = process_recslam_specific_data(session_data)

    return data_to_collection

def process_recslam_specific_data(session_data: dict) -> dict:
    data_to_collection = {}

    url = f"gs://roadly-{session_data['platform']}-videos/{session_data['folder_prefix']}/{session_data['folder_name']}"
    files = get_files_in_gcloud_bucket(url)

    mapping = {
        'motion.csv': 'IMU',
        'detections.json': 'detection',
        'snapshots.zip_': 'snapshots',
        'gps.csv': 'raw_gps',
        'device.txt': 'device',
        'heading.csv': 'heading',
        'logs.zip': 'logs',
    }

    for file in files:
        name = mapping.get(file)
        if name:
            url_with_file = f'{url}/{file}'
            if file == 'gps.csv':
                data_to_collection.setdefault('track', {})[name] = url_with_file
            else:
                data_to_collection[name] = url_with_file
        elif not file.startswith(('times', 'video')):
            data_to_collection.setdefault('other', []).append(f'{url}/{file}')

    track_line = session_data.get('track_line', {})
    if track_line:
        geometry = track_line.get('geometry', {})
        if geometry:
            data_to_collection.setdefault('track', {})[geometry['type']] =  geometry['coordinates']

    return data_to_collection

def prepare_data_for_pipelines_collection(pipeline_data: dict) -> dict:
    if 'definition' not in pipeline_data or 'platform' not in pipeline_data or 'uuid' not in pipeline_data or 'pipeline_type' not in pipeline_data or 'status' not in pipeline_data or 'created' not in pipeline_data or 'updated' not in pipeline_data or 'is_public' not in pipeline_data:
        raise ValueError("pipeline_data does not contain the expected keys")
    module_collection = {}
    if pipeline_data['definition']:
        folder_prefix = pipeline_data['definition'][0]['id']
        folder_name = pipeline_data['definition'][0]['module']
        module_collection['folder_url'] = f'gs://roadly-{pipeline_data["platform"]}-pipelines/{folder_prefix}/{folder_name}'

    for i, pipeline in enumerate(pipeline_data['definition']):
        key = f"{pipeline['module']}"
        value = {f'{pipeline["run_section"]}': [f'{module_collection["folder_url"]}/{pipeline["id"]}']}
        if key in module_collection:
            if pipeline["run_section"] in module_collection[key]:
                module_collection[key][pipeline["run_section"]].extend(value[pipeline["run_section"]])
            else:
                module_collection[key].update(value)
        else:
            module_collection[key] = value

    data_to_collection = {
        'platform': pipeline_data['platform'],
        'pipeline_id': pipeline_data['uuid'],
        'pipeline_type': pipeline_data['pipeline_type'],
        'meta': {
            'status': pipeline_data['status'],
            'created': pipeline_data['created'],
            'updated': pipeline_data['updated'],
            'is_public': pipeline_data['is_public']
        },
        'modules': module_collection
    }
    
    return data_to_collection


def prepare_data_to_image_collection(pipeline_data: dict) -> dict:
    data_to_collection = {}  
    platform = pipeline_data['platform']
    pipeline_id = pipeline_data['uuid']
    url = f"gs://roadly-{platform}-rda/test-1/{pipeline_id}/track_estimation_out/frontal/input/image/frame/compressed"
    images = get_files_in_gcloud_bucket(url)
    
    folder_url = pipeline_data['definition'][0]['arguments'][0]['value'].split('/')
    folder_name = folder_url[-1]
    
    data_to_collection.update({
        'source': folder_name,
        'platform': platform,
        'meta': 'No images' if not images else {'url': url, 'image_names': images},
        'cvat_task': None
    })

    return data_to_collection 

def build_video_collection(session_data, url, files, camera_index, camera_name):
    video_collection = {}
    video_collection['source'] = 'RecSlam' if len(session_data['folder_prefix']) > 50 else session_data['folder_prefix']
    video_collection['url'] = f'{url}/video' if camera_name == 'ULTRAWIDE' else f'{url}/video_2'
    video_collection['session_id'] = session_data['id']
    video_collection['platform'] = session_data['platform']
    video_collection['camera_name'] = camera_name

    times_file = 'times_2.txt' if camera_name == 'WIDE' else 'times.txt'
    times_full_file = 'times_full_2.json' if camera_name == 'WIDE' else 'times_full.json'
    time_files = [f'{url}/{file}' for file in [times_file, times_full_file] if file in files]
    camera_data = session_data['cameras'][camera_index] if camera_index < len(session_data['cameras']) else None
    if camera_data:
        meta = {}
        for key in ['frame_count', 'fps', 'width', 'height', 'start_time', 'end_time']:
            meta[key] = camera_data.get(key)
        meta['duration'] = get_duration(meta['start_time'], meta['end_time'])
        meta['time_files'] = time_files
        video_collection['meta'] = meta

    return video_collection

def prepare_data_to_video_collection(session_data: dict) -> list[dict]:
    platform = session_data['platform']
    folder_prefix, folder_name = session_data['folder_prefix'], session_data['folder_name']
    url = f"gs://roadly-{platform}-videos/{folder_prefix}/{folder_name}"
    files = get_files_in_gcloud_bucket(url)

    if 'video' not in files:
        return [{}, {}]
    
    video_collection_wide = {}
    video_collection_ultrwide = build_video_collection(session_data, url, files, 0, 'ULTRAWIDE')

    if session_data['is_multicam'] and 'video' in files and 'video_2' in files:
        video_collection_wide = build_video_collection(session_data, url, files, 1, 'WIDE')
        video_collection_ultrwide = build_video_collection(session_data, url, files, 0, 'ULTRAWIDE')

    return video_collection_wide, video_collection_ultrwide

def get_duration(start_time: str, end_time: str) -> str:
    try:
        start = datetime.strptime(start_time, '%Y-%d-%m %H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%d-%m %H:%M:%S')
    except:
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    return str(end - start)

    