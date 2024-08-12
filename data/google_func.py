import os
import glob
import datetime

import pandas as pd
import plotly.graph_objects as go

from data.logger import Logger
from data.google import GoogleSpreadsheet
from data.cvat import CVATConnection
from data.cvat_func import get_task_jobs_mapping, download_annotations, decode_response_data_to_json, annotations_for_preview
from cvat_sdk.api_client import Configuration


logger = Logger('spreadsheet.log')
google_spreadsheet_dataset = GoogleSpreadsheet('SPREADSHEET_URL_DATASET')
google_spreadsheet_control = GoogleSpreadsheet('SPREADSHEET_URL_CONTROL')
cvat_config = CVATConnection().config


def get_jobs_task_mapping(task_job_mapping: dict[int, list[int]]) -> dict[int, int]:
    job_task_mapping = {}
    for task_id, job_ids in task_job_mapping.items():
        for job_id in job_ids:
            job_task_mapping[job_id] = task_id
    return job_task_mapping

def update_validation_params(update_params: dict[str, list], job: int, row: dict[str, any],
                              google_spreadsheet_dataset: any, logger: any) -> None:
    validation_df = google_spreadsheet_dataset.get_worksheet_data('Validation')
    validation_rows = validation_df.index[validation_df['Job'] == job].tolist()
    if validation_rows:
        validation_row_number = validation_rows[0]
        update_params.setdefault('Validation', []).append([validation_row_number + 2, 'Validation status', row['Validation status']])
    else:
        logger.log(f"Job {job} not found in Validation worksheet")

def update_protocol_params(update_params: dict[str, list], job: int, protocol: str,
                            row: dict[str, any], google_spreadsheet_dataset: any, logger: any) -> None:
    if protocol in ['6', '6.1']:
        protocol_df = google_spreadsheet_dataset.get_worksheet_data('Protocol 6')
    elif protocol in ['3', '3.1', '3.7']:
        protocol_df = google_spreadsheet_dataset.get_worksheet_data('Protocol 3')
    elif protocol == '10':
        protocol_df = google_spreadsheet_dataset.get_worksheet_data('Protocol 10')
    else:
        logger.log(f"Invalid protocol {protocol} for job {job}")
        return
    
    protocol_rows = protocol_df.index[protocol_df['Job'] == job].tolist()
    if protocol_rows:
        protocol_row_number = protocol_rows[0]
        update_params.setdefault(f'Protocol {protocol}', []).append([protocol_row_number + 2, 'Job status', row['Job status']])
    else:
        logger(f"Job {job} not found in Protocol {protocol} worksheet")

def prepare_update_params(df_complete: pd.DataFrame) -> dict[str, list]:
    update_params = {}

    for index, row in df_complete.iterrows():
        job = row['Job']
        protocol = row['Protocol']

        update_validation_params(update_params, job, row)
        update_protocol_params(update_params, job, protocol, row)

    return update_params

def get_df_to_valid(annotation_worksheet: pd.DataFrame, validation_worksheet: pd.DataFrame,
                     task_ids: list[int]=None) -> pd.DataFrame:
    job_valid_list = validation_worksheet['Job'].tolist()
    if task_ids:
        task_ids = [str(i) for i in task_ids]
        df_to_valid = annotation_worksheet.loc[(annotation_worksheet['Job status'] == 'To validation')
                                & (~annotation_worksheet['Job'].isin(job_valid_list))
                                & (annotation_worksheet['Task_id'].isin(task_ids))]
    else:
        df_to_valid = annotation_worksheet.loc[(annotation_worksheet['Job status'] == 'To validation')
                                & (~annotation_worksheet['Job'].isin(job_valid_list))]
    df_to_valid = df_to_valid[['Task', 'Job', 'Operator']]
    return df_to_valid

def to_validation(google_spreadsheet_dataset: any, task_ids: list[int]=None, priority: int=1,
                   protocol: int=2, clipboard: bool=True) -> None:
    # Fetch data from worksheets
    annotation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Annotation')
    validation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Validation')
    
    df_to_valid = get_df_to_valid(annotation_worksheet, validation_worksheet, task_ids)

    lenth = len(df_to_valid)
    # Add columns to the 'Validation' worksheet
    df_to_valid.insert(0, 'Priority', [priority] * lenth)
    df_to_valid.insert(3, 'Protocol', [protocol] * lenth)
    df_to_valid.insert(4, 'Description',['Ручная разметка'] * lenth)
    df_to_valid.insert(6, 'Supervisor', ['Unassigned'] * lenth)
    df_to_valid.insert(7, 'Validation status', ['Planned'] * lenth)

    if clipboard:
        return df_to_valid.to_clipboard(index=False, header=False)
    else:
        # Update the 'Validation' worksheet with the processed data
        google_spreadsheet_dataset.add_dataframe_to_worksheet(df_to_valid, 'Validation')

def get_df_to_reject(validation_worksheet: pd.DataFrame, rejected_worksheet: pd.DataFrame) -> pd.DataFrame:
    job_reject_list = rejected_worksheet['Job'].to_list()
    df_to_reject = validation_worksheet.loc[(validation_worksheet['Validation status'] == 'Rejected')
                                & (~validation_worksheet['Job'].isin(job_reject_list))]
    # Select columns
    df_to_reject = df_to_reject.iloc[:, [0, 1, 2, 5, 6, 8, 9, 10]]

    df_to_reject.insert(5, 'Job status', ['Planned'] * len(df_to_reject))
    return df_to_reject

def to_rejected(google_spreadsheet_dataset: any, clipboard: bool=True) -> None:
    # Fetch data from worksheets
    rejected_worksheet = google_spreadsheet_dataset.get_worksheet_data('Rejected')
    validation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Validation')

    df_to_reject = get_df_to_reject(validation_worksheet, rejected_worksheet)
    if clipboard:
        return df_to_reject.to_clipboard(index=False, header=False)
    else:
        # Update the 'Rejected' worksheet with the processed data
        google_spreadsheet_dataset.add_dataframe_to_worksheet(df_to_reject, 'Rejected')

def update_cell(worksheet: any, params: list, worksheet_name: str, logger: any) -> None:
    try:
        worksheet.update_cell(params[0], params[1], 'Finished')
        logger.log(f"Updated cell at row {params[0]}, column {params[1]} in worksheet {worksheet_name} to 'Finished'")
    except Exception as e:
        logger.log(f"Failed to update cell at row {params[0]}, column {params[1]} in worksheet {worksheet_name}: {str(e)}")

def delete_row(worksheet: any, params: list, worksheet_name: str, logger: any) -> None:
    try:
        worksheet.delete_rows(params[0])
        logger.log(f"Deleted row {params[0]} from worksheet {worksheet_name}")
    except Exception as e:
        logger.log(f"Failed to delete row {params[0]} from worksheet {worksheet_name}: {str(e)}")


def processing_spreadsheets(google_spreadsheet_dataset: any, logger: any, 
                     update_params: dict[str, list], action: str='update') -> None:
    '''Update or delete data in worksheets'''
    # Iterate by worksheet in update_params
    for worksheet_name, params_list in update_params.items():
        worksheet = google_spreadsheet_dataset.get_worksheet(worksheet_name)
        if worksheet is None:
            logger.log(f"Worksheet {worksheet_name} not found")
            continue

        for params in params_list:
            if action == 'update' and worksheet_name != 'Protocol 10':
                update_cell(worksheet, params, worksheet_name, logger)
            elif action == 'delete':
                delete_row(worksheet, params, worksheet_name, logger)
              
def get_finished_dataframes(rejected_worksheet: pd.DataFrame, validation_worksheet: pd.DataFrame) -> pd.DataFrame:
    df_rejected_finished = rejected_worksheet[rejected_worksheet['Job status'] == 'Finished']
    df_validation_finished = validation_worksheet[validation_worksheet['Validation status'] == 'Finished']

    df_rejected_finished = df_rejected_finished[['Task', 'Job', 'Operator', 'Job status', 'Validation quality']]
    df_validation_finished = df_validation_finished[['Task', 'Job', 'Operator', 'Validation status', 'Validation quality']]

    df_validation_finished.rename(columns={'Validation status': 'Job status'}, inplace=True)

    df_complete = pd.concat([df_rejected_finished, df_validation_finished])
    return df_complete

def merge_annotation_data(df_complete: pd.DataFrame, annotation_worksheet: pd.DataFrame, job_complete_list: list) -> pd.DataFrame:
    # Merge data from the 'Annotation' worksheet
    df_annotation = annotation_worksheet[['Job_id', 'Protocol', 'Comments annotation']]
    df_annotation_finished = annotation_worksheet.loc[(annotation_worksheet['Job status'] == 'Finished')&
                                                        (~annotation_worksheet['Job'].isin(job_complete_list))]
    df_complete = df_complete.merge(df_annotation, how='left', left_on='Job', right_on='Job_id')
    df_complete.columns = ['Job_id', 'Protocol', 'Task', 'Job', 'Operator', 'Validation quality', 'Job status', 'Comments annotation']
    df = pd.concat([df_complete, df_annotation_finished])
    return df

def to_complete(google_spreadsheet_dataset: any, clipboard: bool=True, action: str='update') -> None:
    # Fetch data from worksheets
    rejected_worksheet = google_spreadsheet_dataset.get_worksheet_data('Rejected')
    validation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Validation')
    complete_worksheet = google_spreadsheet_dataset.get_worksheet_data('Complete')
    annotation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Annotation')

    df_complete = get_finished_dataframes(rejected_worksheet, validation_worksheet)

    job_complete_list = complete_worksheet['Job'].tolist()
    df_complete = df_complete.loc[~df_complete['Job'].isin(job_complete_list)]
    df = merge_annotation_data(df_complete, annotation_worksheet, job_complete_list)

    params = prepare_update_params(df_complete)
    processing_spreadsheets(params, action=action)

    if clipboard:
        return df.to_clipboard(index=False, header=False)
    else:
        # Update the 'Complete' worksheet with the processed data
        google_spreadsheet_dataset.add_dataframe_to_worksheet(df, 'Complete')
    
def shuttle(df):
    grouped_df = df.groupby(["Task_id"])
    priorities = [1, 2, 3, 4, 5]
    n_splits = len(priorities)

    final_splits = {priority: pd.Index([]) for priority in priorities}
    for name, group in grouped_df:
        n_rows = len(group) // n_splits
        current_remainder = len(group) % n_splits

        # Shuffle the rows in the group
        shuffled_group = group.sample(frac=1).index
        start_ind = 0
        for priority in priorities:
            end_ind = start_ind + n_rows + (1 if current_remainder > 0 else 0)
            current_remainder -= 1
            priority_group = shuffled_group[start_ind:end_ind]
            final_splits[priority] = final_splits[priority].append(
                priority_group)
            start_ind = end_ind
            if end_ind >= len(group):
                break
    for priority, index in final_splits.items():
        df.loc[index, "Priority"] = priority
    return df

def get_job_list(google_spreadsheet_dataset: any, cvat_config: Configuration, project_id: int, task_ids: list[int]=None) -> list[int]:
    annotation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Annotation')
    job_task_mapping = get_jobs_task_mapping(get_task_jobs_mapping(cvat_config, project_id, task_ids))
    return list(set(job_task_mapping.keys()) - set([int(_) for _ in annotation_worksheet['Job_id'].tolist()]))

def get_protocol(project_id: int) -> float:
    if project_id == 26:
        return 3.1
    elif project_id == 28:
        return 6.1
    elif project_id == 24:
        return 10
    else:
        return float(input('Enter protocol number: '))

def create_dataframe(job_list: list[int], job_task_mapping: dict, priority: int=None, project_id: int=None) -> pd.DataFrame:
    df = pd.DataFrame()
    if len(job_list) != 0:
        df["Job_id"] = job_list
        df["Job"] = [f'https://cvat.kva-kva.top/tasks/{job_task_mapping[job]}/jobs/{job}' for job in job_list]
        df["Task"] = [f'https://cvat.kva-kva.top/tasks/{job_task_mapping[job]}' for job in job_list]
        df["Operator"] = "Unassigned"
        df["Job status"] = "Planned"
        df["Task_id"] = [job_task_mapping[job] for job in job_list]
        df["Priority"] = priority if priority else shuttle(df.copy())["Priority"]
        df["Protocol"] = get_protocol(project_id)
        df = df[['Job_id', 'Task_id', 'Priority', 'Protocol', 'Task', 'Job', 'Operator', 'Job status']]
    return df

def get_worksheet_name(protocol: float) -> str:
    if protocol == 3.1:
        return 'Protocol 3'
    elif protocol == 6.1:
        return 'Protocol 6'
    elif protocol == 10:    
        return 'Protocol 10'
    else:
        logger.log(f"Invalid protocol {protocol}")

def add_new_job_annotation(google_spreadsheet_dataset: any, cvat_config: Configuration, project_id: int,
                            task_ids: list[int]=None, priority: int=None,
                            clipboard: bool=True) -> None:
    job_list = get_job_list(google_spreadsheet_dataset, cvat_config, project_id, task_ids)
    df = create_dataframe(job_list, get_jobs_task_mapping(get_task_jobs_mapping(cvat_config, project_id, task_ids)), priority, project_id)
    if clipboard:
        df.to_clipboard(index=False, header=False)
    else:
        worksheet_name = get_worksheet_name(df["Protocol"].iloc[0])
        google_spreadsheet_dataset.add_dataframe_to_worksheet(df, worksheet_name)

def add_new_job_validation(google_spreadsheet_dataset: any, cvat_config: Configuration, project_id: int,
                            task_ids: list[int]=None, priority: int=None,
                            clipboard: bool=True) -> None:
    '''TODO: test and fix logic'''
    job_list = get_job_list(google_spreadsheet_dataset, cvat_config, project_id, task_ids)
    df = create_dataframe(job_list, get_jobs_task_mapping(cvat_config, project_id, task_ids), priority, project_id)
    df["Job status"] = "Planned"
    df["Description"] = "Ручная разметка"
    df["Supervisor"] = "Unassigned"
    df["Validation status"] = "To validation"
    df['Protocol'] = 2
    df = df[[
            'Priority', 'Task', 'Job', 'Protocol', 'Description', 'Operator',
            'Supervisor', 'Job status'
        ]]
    if clipboard:
        df.to_clipboard(index=False, header=False)
    else:
        google_spreadsheet_dataset.add_dataframe_to_worksheet(df, 'Validation')

def hist(df):
    hist_data = go.Histogram(x=df['name_label'], name='name_label')

    traces = []
    for severity in df['severity'].unique():
        if pd.isna(severity):
            continue
        trace_data = df[df['severity'] == severity]['name_label']
        trace = go.Histogram(x=trace_data, name=f'severity {severity}')
        traces.append(trace)

    layout = go.Layout(title='Histogram of distress distribution', barmode='stack')

    fig = go.Figure(data=[hist_data] + traces, layout=layout)

    fig.show()

def take_annotation(cvat_config: Configuration, job_list: list[int]) -> pd.DataFrame:
    annotation = []
    for job in job_list:
        data = download_annotations(cvat_config, 'jobs_api', job)
        json_data = decode_response_data_to_json(data)
        annotation.extend(annotations_for_preview(json_data))
    return pd.DataFrame(annotation)

def preview_new_data(project_id: int, task_id: list, cvat_config: Configuration):
    task_job_mapping = get_task_jobs_mapping(cvat_config, project_id, task_id)
    job_list = [job for jobs in task_job_mapping.values() for job in jobs]
    df = take_annotation(cvat_config, job_list)
    return hist(df)

def get_latest_file(save_path):
    files = glob.glob(os.path.join(save_path, '*'))
    latest_file = max(files, key=os.path.getctime)
    return latest_file

def min_time_estimate(row):
    if row['Protocol'] in ['6', '6.1']:
        val = 2
    elif row['Protocol'] in ['3.1', '3']:
        val = 1
    elif row['Protocol'] in ['10', '3.7']:
        val = 0.5
    else:
        print(f'Protocol: {row["Protocol"]}')
        val = input('Enter min time: ')
    return val

def max_time_estimate(row):
    if row['Protocol'] in ['6.1', '6']:
        val = 4
    elif row['Protocol'] in ['3.1', '3', '3.7', '10']:
        val = 2
    else:
        print(f'Protocol: {row["Protocol"]}')
        val = input('Enter max time: ')
    return val

def filter_by_job_status(df, statuses):
    return df[df['Job status'].isin(statuses)]

def get_diff_df(df1, df2):
    return pd.merge(df1, df2, how='outer', indicator='Exist').loc[lambda x : x['Exist'] != 'both']

def update_operator_worksheet(df, operator, date, google_spreadsheet_control):
    df_update = df[df['Operator'] == operator]
    df_update = df_update[['Job', 'Protocol']]
    df_update.columns = ['Task', 'Protocol']
    df_update.insert(0, 'Data', [str(date)] * len(df_update))
    df_update['min time estimate'] = df_update.apply(min_time_estimate, axis=1)
    df_update['max time estimate'] = df_update.apply(max_time_estimate, axis=1)
    google_spreadsheet_control.add_dataframe_to_worksheet(df_update, operator)

def work_control(google_spreadsheet_dataset, google_spreadsheet_control, save_path):
    annotation_worksheet = google_spreadsheet_dataset.get_worksheet_data('Annotation')
    today = datetime.date.today()
    old_date_file = get_latest_file(save_path)
    annotation_worksheet_old = pd.read_csv(f'{old_date_file}', dtype=object)

    names = ['Job_id', 'Task_id', 'Priority', 'Protocol', 'Task', 'Job', 'Operator', 'Job status']
    annotation_worksheet = annotation_worksheet[names]
    annotation_worksheet_old = annotation_worksheet_old[names]

    annotation_worksheet_old = filter_by_job_status(annotation_worksheet_old, ['Finished', 'To validation', 'Intermediate'])
    annotation_worksheet = filter_by_job_status(annotation_worksheet, ['Finished', 'To validation', 'Intermediate'])

    diff_df = get_diff_df(annotation_worksheet, annotation_worksheet_old)
    print(len(diff_df))

    operators = diff_df['Operator'].unique().tolist()
    job_per_day_worksheet = google_spreadsheet_control.get_worksheet_data('jobs per day')

    counter = {'Data': str(today)}
    for operator in operators:
        print(operator)
        counter[operator] = len(diff_df[diff_df['Operator'] == operator])
        print(counter[operator])
        update_operator_worksheet(diff_df, operator, today, google_spreadsheet_control)

    print(counter)
    google_spreadsheet_control.update_worksheet('jobs per day', counter)

    with open(f'{save_path}/{today}.csv', 'w') as f:
        f.write(annotation_worksheet.to_csv(index=False))


