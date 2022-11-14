import os
import json
import boto3
from datetime import datetime, timedelta
from googleads import ad_manager
from googleads import errors
from zeep import helpers
import pandas as pd
import numpy as np
import tempfile
import pytz
from ga_process.utils import (
    get_data_from_dynamodb,
    write_file_to_s3,
    error_notify_v2
)

from logging_setup_etl import setup_logging

logger = setup_logging('logging_conf.json')

DATA_SOURCE = 'ad_manager'

generic_column_list = ["generic1", "generic2", "generic3", "generic4",
                       "generic5", "generic6", "generic7", "generic8",
                       "generic9", "generic10"]

def add_generic_and_timestamp(df, date_for_ts, frequency_type):
    for col in generic_column_list:
        df[col] = None
    if frequency_type.lower() == 'hist':
        df['insrt_ts'] = date_for_ts + timedelta(seconds=1)
        return df
    df['insrt_ts'] = (datetime.now()).strftime(
        '%Y-%m-%d %H:%M:%S')
    return df

def change_float_columns_to_int(df):
    for col in df.columns.tolist():
        if df[col].dtype == 'float64':
            df[col].fillna(0.0, inplace=True)
            df[col] = df[col].astype(int)
    return df
def update_item_in_dynamodb(table_name, source_name, final_cols_list, alias, pos):
    dynamodb = boto3.resource('dynamodb',
                              region_name='ap-southeast-1')
    table = dynamodb.Table(table_name)
    table.update_item(
        Key={'source': source_name},
        UpdateExpression="set services[{}].report_job_columns = :cols".format(pos),
        ConditionExpression="services[{}].alias = :a".format(pos),
        ExpressionAttributeValues={
            ':a': str(alias),
            ':cols': final_cols_list
        },
        ReturnValues="UPDATED_NEW"
    )
def get_api_reports(alias, item, yaml_path, date, frequency_type,output_path, output_bucket, pos):
    # For ad_unit, creative and line_item_creative api reports

    logger.info("Working on api report for {}".format(alias))
    AdManager_client = ad_manager.AdManagerClient.LoadFromStorage(yaml_path)
    report_downloader = AdManager_client.GetDataDownloader(item.get('version'))

    report_job = item.get('report_job')
    report_job['reportQuery']['endDate']['day'] = report_job['reportQuery']['startDate']['day'] = date.day
    report_job['reportQuery']['endDate']['month'] = report_job['reportQuery']['startDate']['month'] = date.month
    report_job['reportQuery']['endDate']['year'] = report_job['reportQuery']['startDate']['year'] = date.year
    try:
        logger.info("Fetching report for {}".format(alias))
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        logger.error('Failed to generate report. Error was:{}'.format(e))
    try:

        export_format = 'CSV_DUMP'
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False, dir='./')
        report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file)

        report_file.close()
        df = pd.read_csv(report_file.name, compression='gzip', error_bad_lines=False)
        df_fixed = df.replace('-', np.nan)
        try:
            df_fixed.columns = item.get('report_job_columns')
        except Exception as e:
            dynamo_cols = item.get('report_job_columns')
            dynamo_cols = [str(y) for y in dynamo_cols]
            df_fixed.columns = df_fixed.columns.str.replace(' ', '_')
            df_fixed.columns = [x.lower() for x in df_fixed.columns]
            df_fixed.columns = [x.split('.')[1] if '.' in x else x for x in df_fixed.columns]
            added_cols = list(df_fixed.columns)
            deleted_cols = []
            for col in dynamo_cols:
                if col in added_cols:
                    added_cols.remove(col)
                else:
                    deleted_cols.append(col)
            if len(added_cols) != 0:
                for add_col in added_cols:
                    dynamo_cols.append(add_col)
                update_item_in_dynamodb('cleansing_rules_config', 'ad_manager', dynamo_cols, alias, pos)
            for del_col in deleted_cols:
                df_fixed[del_col] = None

            if len(added_cols) > 0:
                e = 'Added {} new columns in DynamoDB for report {} which are : {}.'.format(
                    str(len(added_cols)), alias, added_cols)
                error_notify_v2(DATA_SOURCE, notification_type='Completedwitherrors',
                                schedule_type='scheduled',
                                _date=date,
                                notification_time=date,
                                message=str(e))

            if len(deleted_cols) > 0:
                e = '{} columns are not present in report {} which are : {}.'.format(
                    str(len(deleted_cols)), alias, deleted_cols)
                error_notify_v2(DATA_SOURCE, notification_type='Completedwitherrors',
                                schedule_type='scheduled',
                                _date=date,
                                notification_time=date,
                                message=str(e))
            df_fixed = df_fixed[dynamo_cols]
        df_trans = add_generic_and_timestamp(df_fixed, date, frequency_type)
        df_type_reformat = change_float_columns_to_int(df_trans)
        df_expand_csv = df_type_reformat.to_csv(index=False, sep='|')
        expand_output_path = output_path.replace('json', 'csv').replace('ad_units',
                                                 'ad_units_report').replace \
            ('/creative/', '/creative_report/') \
            .replace('line_item_creative', 'line_item_creative_ad_units_report').replace('ad_unit_creative', 'ad_unit_creative_report')
        write_file_to_s3(df_expand_csv, output_bucket, expand_output_path,
                         content_type='text/csv')
        logger.info('File written at {}'.format(expand_output_path))
    except Exception as e:
        print(e)
        error_notify_v2(DATA_SOURCE, notification_type='Completedwitherrors',
                        schedule_type='scheduled',
                        _date=date,
                        notification_time=date,
                        message=str(e))
    os.remove(report_file.name)
    logger.info('file removed for {}'.format(alias))

def get_statement_value(service_name, data_frequency):
    now = datetime.now(tz=pytz.timezone('Asia/Kuala_Lumpur'))
    if data_frequency.lower() == 'all':
        return ad_manager.StatementBuilder(version='v201811')

    if service_name == 'LineItemService':
        statement = (ad_manager.StatementBuilder(version='v201811')
                     .Where(('endDateTime > :now '))
                     .WithBindVariable('now', now))
        return statement

    elif service_name == 'LineItemCreativeAssociationService':
        statement = (ad_manager.StatementBuilder(version='v201811')
                     .Where(('endDateTime > :now '))
                     .WithBindVariable('now', now))
        return statement

    elif service_name == 'OrderService':
        statement = (ad_manager.StatementBuilder(version='v201811')
                     .Where(('endDateTime > :now '))
                     .WithBindVariable('now', now))
        return statement

    elif service_name == 'ProposalService':
        statement = (ad_manager.StatementBuilder(version='v201811')
                     .Where(('endDateTime > :now '))
                     .WithBindVariable('now', now))
        return statement

    elif service_name == 'ProposalLineItemService':
        statement = (ad_manager.StatementBuilder(version='v201811')
                     .Where(('endDateTime > :now '))
                     .WithBindVariable('now', now))
        return statement
    else:
        return ad_manager.StatementBuilder(version='v201811')


def get_response_from_api(client, service_name, version, method_name,
                          statement_value):
    service_client_obj = client.GetService(service_name, version=version)
    response = getattr(service_client_obj, method_name)(statement_value)
    return response


def get_custom_targetting_service_response(client):
    custom_targeting_service = client.GetService(
        'CustomTargetingService', version='v201811')
    # Create statement to get all targeting keys.
    targeting_key_statement = ad_manager.StatementBuilder(version='v201811')

    all_keys = []

    # Get custom targeting keys by statement.
    while True:
        response = custom_targeting_service.getCustomTargetingKeysByStatement(
            targeting_key_statement.ToStatement())
        if 'results' in response and len(response['results']):
            all_keys.extend(response['results'])
            targeting_key_statement.offset += targeting_key_statement.limit
        else:
            break
    response_results = []
    if all_keys:
        # Create a statement to select custom targeting values.
        statement = (ad_manager.StatementBuilder(version='v201811')
                     .Where('customTargetingKeyId IN (%s)' %
                            ', '.join([str(key['id']) for key in all_keys])))

        # Retrieve a small amount of custom targeting values at a time, paging
        # through until all custom targeting values have been retrieved.
        while True:
            response = custom_targeting_service.getCustomTargetingValuesByStatement(
                statement.ToStatement())
            if 'results' in response and len(response['results']):
                response_results.extend(response['results'])
                statement.offset += statement.limit
            else:
                break
    return response_results


def process(table_name, ip_date,
            yaml_path, output_bucket, network_id, source_name, data_frequency):
    date = datetime.strptime(ip_date, '%Y/%m/%d')
    year = date.year
    month = date.month
    day = date.day
    monthh = '{:02}'.format(month)
    dayy = '{:02}'.format(day)
    pos = -1
    client = ad_manager.AdManagerClient.LoadFromStorage(yaml_path)
    ad_manager_services = get_data_from_dynamodb(table_name, 'source',
                                                 source_name)

    for item in ad_manager_services['services']:
        logger.info(json.dumps(item))
        try:
            pos += 1
            service_name = item['service_name']
            version = item['version']
            method_name = item['method_name']
            alias = item['alias']
            output_path = "ad-manager/raw/json/{0}/year={1}/month={2}/day={3}/{0}_{1}_{2}_{3}.json".format(alias, year, monthh, dayy)
            if alias == 'ad_units' or alias == 'creative' or alias == 'line_item_creative':
                get_api_reports(alias, item, yaml_path, date, data_frequency, output_path, output_bucket, pos)
            elif alias == "ad_unit_creative":
                get_api_reports( alias, item, yaml_path, date, data_frequency, output_path, output_bucket, pos)
                continue
            statement = get_statement_value(service_name, data_frequency)
            response_results = []
            if service_name == 'CustomTargetingService':
                response_results = get_custom_targetting_service_response(
                    client)
            else:
                while True:
                    statement_value = statement.ToStatement()
                    response = get_response_from_api(client, service_name,
                                                     version,
                                                     method_name,
                                                     statement_value)
                    if not response:
                        logger.warning('No response received')
                        break
                    if 'results' in response and len(response['results']):
                        response_results.extend(response['results'])
                        statement.offset += statement.limit
                    else:
                        break
            if response_results:
                response_results = json.dumps(helpers.serialize_object(
                    response_results), indent=4)
            else:
                response_results = ""
            write_file_to_s3(response_results, output_bucket, output_path,
                             'application/json')
            logger.info('Data written to file {0}'.format(output_path))
        except Exception as e:
            logger.exception(e)
            continue
    return


def main(*args):
    """
  Args 1: Table_name
  Args 2: Date
  Args 3: YAML_File_path
  Args 4: Output_Bucket_name
  Args 5: Network_ID
  Args 6: Source Name
  Args 7: Pipeline_Time
  Args 8: Data Frequency
  """
    try:

        if len(args) < 8:
            logger.error(args)
            raise Exception("Please pass all the arguments in order "
                            "1. Table_name 2. Date 3. YAML_File_path "
                            "4. Output_Bucket_name 5.Network_ID 6. Source Name")
        logger.info(args)
        table_name = args[0]
        date = args[1]
        yaml_path = args[2]
        output_bucket = args[3]
        network_id = args[4]
        source_name = args[5]
        pipeline_time = args[6]
        data_frequency = args[7]

        schedule_type = os.environ.get('SCHEDULE_TYPE', 'scheduled')
        process(table_name, date,
                yaml_path, output_bucket, network_id, source_name,
                data_frequency)

    except Exception as e:
        logger.exception(e)
        error_notify_v2(DATA_SOURCE, notification_type='PipelineFailure',
                        schedule_type=schedule_type,
                        _date=date,
                        notification_time=pipeline_time,
                        message=str(e))
