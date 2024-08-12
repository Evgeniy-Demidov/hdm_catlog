import time
import json
import io
import os
import shutil
import zipfile
import pandas as pd
from tqdm import tqdm

from data.cvat import CVATConnection
from cvat_sdk.api_client import ApiClient, Configuration, exceptions
from data.logger import Logger


cvat_config = CVATConnection().config
logger = Logger('cvat.log')


def get_request_params(api: str, **kwargs) -> dict:
    params = {
        'page_size': 10000,
        'org': 'rda'
    }
    if api == 'jobs_api':
        params.update({
            'x_organization': kwargs.get('x_organization'),
            'assignee': kwargs.get('assignee'),
            'filter': kwargs.get('filter'),
            'frame_id': kwargs.get('frame_id'),
            'org_id': kwargs.get('org_id'),
            'owner': kwargs.get('owner'),
            'page': kwargs.get('page'),
            'resolved': kwargs.get('resolved'),
            'search': kwargs.get('search'),
            'sort': kwargs.get('sort'),
            'task_id': kwargs.get('task_id'),
            'state': kwargs.get('state'),
            'stage': kwargs.get('stage'),
            'project_id': kwargs.get('project_id'),
        })
    elif api == 'tasks_api':
        params.update({
            'x_organization': kwargs.get('x_organization'),
            'assignee': kwargs.get('assignee'),
            'dimension': kwargs.get('dimension'),
            'filter': kwargs.get('filter'),
            'mode': kwargs.get('mode'),
            'name': kwargs.get('name'),
            'org_id': kwargs.get('org_id'),
            'owner': kwargs.get('owner'),
            'page': kwargs.get('page'),
            'project_id': kwargs.get('project_id'),
            'project_name': kwargs.get('project_name'),
            'search': kwargs.get('search'),
            'sort': kwargs.get('sort'),
            'status': kwargs.get('status'),
            'subset': kwargs.get('subset'),
            'tracker_link': kwargs.get('tracker_link'),
        })
    # Remove keys with None values
    params = {k: v for k, v in params.items() if v is not None}
    return params


def get_list_data(cvat_config: Configuration, api: str, **kwargs) -> list[dict]:
    params = get_request_params(api, **kwargs)
    with ApiClient(cvat_config) as client:
        try:
            (data, response) = getattr(client, api).list(**params)
        except exceptions.ApiException as e:
            logger.log(f"Exception when calling {api}.list(): {e}\n")
            return []
    return data.to_dict()['results']


def process_jobs(jobs: list[dict]) -> dict[int, list[int]]:
    task_job_mapping = {}
    for job in jobs:
        task_id = job['task_id']
        job_id = job['id']
        if task_id in task_job_mapping:
            task_job_mapping[task_id].append(job_id)
        else:
            task_job_mapping[task_id] = [job_id]
    return task_job_mapping


def get_task_jobs_mapping(cvat_config: Configuration, project_id: int=None, task_ids: list[int]=None, stage: str=None, state: str=None) -> dict[int, list[int]]:
    if task_ids:
        task_job_mapping = {}
        for task_id in task_ids:
            jobs = get_list_data(cvat_config, 'jobs_api', task_id=task_id, project_id=project_id, stage=stage, state=state)
            for task, job_list in process_jobs(jobs).items():
                if task in task_job_mapping:
                    task_job_mapping[task].extend(job_list)
                else:
                    task_job_mapping[task] = job_list
    else:
        jobs = get_list_data(cvat_config, 'jobs_api', project_id=project_id, stage=stage, state=state)
        task_job_mapping = process_jobs(jobs)
    return task_job_mapping


def process_tasks(tasks: list[dict]) -> dict[int, str]:
    task_pipeline_mapping = {}
    for task in tasks:
        task_id = task['id']
        pipeline_name = task['name']
        if pipeline_name.startswith('auto_'):
            pipeline_name = pipeline_name.split('_')[1]
        task_pipeline_mapping[task_id] = pipeline_name
    return task_pipeline_mapping


def get_task_pipeline_mapping(cvat_config: Configuration, **kwargs) -> dict[int, str]:
    tasks = get_list_data(cvat_config, 'tasks_api', **kwargs)
    task_pipeline_mapping = process_tasks(tasks)
    return task_pipeline_mapping


def download_annotations(cvat_config: Configuration, api: str, id: int) -> bytes:
    with ApiClient(cvat_config) as client:
        output_format="Datumaro 1.0"
        action = "download"
        location = "local"
        use_default_location = False

        while True:
            try:
                _, response = getattr(client, api).retrieve_annotations(
                    format=output_format,
                    id=id,
                    action=action,
                    location=location,
                    use_default_location=use_default_location,
                    _parse_response=False,
                )
                if response.status == 200:
                    time.sleep(1)
                    return response.data
            except exceptions.ApiException as e:
                logger.log(f"Exception when calling {api}.retrieve_dataset(): {e}\n")
                continue


def decode_response_data_to_json(data: bytes) -> dict[str, any]:
    data_io = io.BytesIO(data)

    with zipfile.ZipFile(data_io) as zipf:
        with zipf.open('annotations/default.json') as f:
            json_data = json.load(f)

    return json_data


def annotations_for_preview(json_data: dict[str, any], job_id: int) -> list[dict[str, any]]:
    annotations = []
    class_names = json_data["categories"]["label"]["labels"]
    class_names = [cl["name"] for cl in class_names]
    result_class_names = {
        n: label
        for n, label in enumerate(class_names)
    }
    for frame_id in range(len(json_data["items"])):
        frame_anno = json_data["items"][frame_id]
        for i, distress in enumerate(frame_anno["annotations"]):
            label_id = distress['label_id']
            annotations.append({
                'name_label': result_class_names[label_id],
                'job_id': job_id,
                'severity': distress['attributes'].get('severity', None)
            })
    return annotations

def read_tags_from_file(filename):
    with open(filename, 'r') as file:
        tags = [line.strip() for line in file]
    return tags


tags = read_tags_from_file('./service/tags.txt')


def increment_tag_counter(tag_counter, tag_name):
    tag_counter[tag_name] = tag_counter.get(tag_name, 0) + 1

def process_tags(json_data:  dict[str, any], cat_names: dict) -> dict[str, any]:
    tags_counter = {}
    tags_data = {}
    customs = []

    for frame in json_data["items"]:
        for annotation in frame["annotations"]:
            label_id = annotation['label_id']
            label_name = cat_names[label_id]

            if label_name in ['crossroad', 'blur', 'dividing_line']:
                tags_data[label_name] = 'True'
            elif label_name == 'сustom_tag':
                customs.append(annotation['attributes']['text'])
            elif label_name == 'color_of_road_marking':
                tag_name = f'{label_name}_{annotation["attributes"]["color"]}'
                increment_tag_counter(tags_counter, tag_name)
            elif label_name in tags[:32]:
                increment_tag_counter(tags_counter, label_name)
            elif label_name in tags[32:40]:
                path_name = label_name.split('_')
                path_name[0] = 'Number'
                name = ' '.join(path_name[:4])
                value = path_name[-1]
                tags_data.setdefault(name, []).append(value)
            elif label_name == 'undefined':
                for attr in ['garbage', 'grass', 'puddle', 'sand']:
                    if annotation["attributes"][attr] == 'True':
                        tags_data[attr] = 'True'

    for tag, count in tags_counter.items():
        if count / len(json_data["items"]) > 0.8:
            path_name = tag.split('_')
            path_name[0] = path_name[0].capitalize()
            name = ' '.join(path_name[:-1])
            value = path_name[-1].capitalize()
            tags_data[name] = value

    if customs:
        tags_data['Custom'] = customs

    return tags_data

def parse_annotation(json_data: dict[str, any], cat_names: dict) -> list[dict[str, any]]:
    tag_job = []
    distress_job = []
    
    for num, frame_anno in enumerate(json_data["items"]):
        tag_frame = []
        distress_frame = []
        frame_id = frame_anno['attr']['frame']

        for distress in frame_anno["annotations"]:
            label_name = cat_names[distress['label_id']]
            annotation = {
                'label_name': label_name,
                'type': distress['type'],
                'attributes': distress['attributes']
            }

            if label_name in tags:
                tag_frame.append(annotation)
            else:
                distress_frame.append(annotation)

        tag_job.append({'frame': frame_id, 'annotations': tag_frame})
        distress_job.append({'frame': frame_id, 'annotations': distress_frame})

    return tag_job, distress_job
    