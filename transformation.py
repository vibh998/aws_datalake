import os
import pandas as pd
from datetime import datetime, timedelta
from StringIO import StringIO
from logging_setup_etl import setup_logging
from functools import reduce
from googleads import ad_manager
from googleads import errors
import sys
import tempfile
from ga_process.utils import (
    get_s3_object,
    write_file_to_s3,
    error_notify_v2,
    get_data_from_dynamodb
)
import boto3
from pandas.io.json import json_normalize
import json
import numpy as np
from boto3.dynamodb.conditions import Key

reload(sys)
sys.setdefaultencoding('utf8')
logger = setup_logging('logging_conf.json')

DATA_SOURCE = 'ad_manager'

generic_column_list = ["generic1", "generic2", "generic3", "generic4",
                       "generic5", "generic6", "generic7", "generic8",
                       "generic9", "generic10"]

startdate_filter_services = ['line_item', 'line_item_creative', 'proposal', 'proposal_line_item']


def only_dict(d):
    '''
    Convert json string representation of dictionary to a python dict
    '''
    if isinstance(d, dict) is True or not d:
        return d
    return json.loads(d)


def extend_columns_in_rows(df, col_target):
    # Flatten columns of lists
    col_flat = [item for sublist in df[col_target] for item in sublist]
    # Row numbers to repeat
    lens = df[col_target].apply(len)
    vals = range(df.shape[0])
    ilocations = np.repeat(vals, lens)
    # Replicate rows and add flattened column of lists
    cols = [i for i, c in enumerate(df.columns) if c != col_target]
    new_df = df.iloc[ilocations, cols].copy()
    new_df[col_target] = col_flat
    return new_df


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


def update_delivery_data(row, table_name, alias):
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
    table = dynamodb.Table(table_name)
    ts = str(row['insrt_ts'])
    if alias == 'line_item':
        id = row['order_id']
        reference_id = str(row['line_item_id']) + "_" + alias
        table_data = table.query(
            KeyConditionExpression=Key('_id').eq(id) & Key('reference_id').eq(reference_id))
        row['generic1'] = row['impressions_delivered']
        row['generic2'] = row['video_completions_delivered']
        row['generic3'] = row['video_starts_delivered']
        row['generic4'] = row['viewable_impressions_delivered']
        row['generic5'] = row['clicks_delivered']
        if table_data['Items']:
            row['generic1'] = row['impressions_delivered'] - table_data['Items'][0].get('impressions_delivered_value')
            row['generic2'] = row['video_completions_delivered'] - table_data['Items'][0].get('video_completions_delivered_valuet')
            row['generic3'] = row['video_starts_delivered'] - table_data['Items'][0].get('video_starts_delivered_value')
            row['generic4'] = row['viewable_impressions_delivered'] - table_data['Items'][0].get(
                'viewable_impressions_delivered_value')
            row['generic5'] = row['clicks_delivered'] - table_data['Items'][0].get('clicks_delivered_value')

        response = table.update_item(
            Key={
                "_id": id,
                "reference_id": reference_id
            },
            UpdateExpression=" set impressions_delivered_value = :si," \
                             " video_completions_delivered_valuet =:vc," \
                             " video_starts_delivered_value =:vs," \
                             " viewable_impressions_delivered_value = :vi,"
                             " clicks_delivered_value = :cd,"
                             " time_stamp = :ts",
            ExpressionAttributeValues={
                ':si': row['impressions_delivered'],
                ':vc': row['video_completions_delivered'],
                ':vs': row['video_starts_delivered'],
                ':vi': row['viewable_impressions_delivered'],
                ':cd': row['clicks_delivered'],
                ':ts': ts
            })
        logger.info("Record updated in table for {}".format(alias))
        return row

    if alias == 'line_item_creative':
        id = row['line_item_id']
        reference_id = str(row['creative_id']) + "_" + alias
        table_data = table.query(
            KeyConditionExpression=Key('_id').eq(id) & Key('reference_id').eq(reference_id))
        creativeset_clicks_delivered =int(0 if row['creativeset_clicks_delivered'] is None else
                                          row['creativeset_clicks_delivered'])
        creativeset_impressions_delivered =int(0 if row['creativeset_impressions_delivered'] is None else
                                          row['creativeset_impressions_delivered'])
        creativeset_video_completions_delivered =int(0 if row['creativeset_video_completions_delivered'] is None else
                                                     row['creativeset_video_completions_delivered'])
        creativeset_video_starts_delivered = int(0 if row['creativeset_video_starts_delivered'] is None else
                                                 row['creativeset_video_starts_delivered'])
        creativeset_viewable_impressions_delivered =int(0 if row['creativeset_viewable_impressions_delivered'] is None
                                                        else row['creativeset_viewable_impressions_delivered'])
        clicks_delivered = int(0 if row['clicks_delivered'] is None else row['clicks_delivered'])
        impressions_delivered = int(0 if row['impressions_delivered'] is None else row['impressions_delivered'])
        video_completions_delivered =int(0 if row['video_completions_delivered'] is None else
                                         row['video_completions_delivered'])
        video_starts_delivered = int(0 if row['video_starts_delivered'] is None else row['video_starts_delivered'])
        viewable_impressions_delivered = int(0 if row['viewable_impressions_delivered'] is None else
                                             row['viewable_impressions_delivered'])

        row['generic1'] = creativeset_clicks_delivered
        row['generic2'] = creativeset_impressions_delivered
        row['generic3'] = creativeset_video_completions_delivered
        row['generic4'] = creativeset_video_starts_delivered
        row['generic5'] = creativeset_viewable_impressions_delivered
        row['generic6'] = clicks_delivered
        row['generic7'] = impressions_delivered
        row['generic8'] = video_completions_delivered
        row['generic9'] = video_starts_delivered
        row['generic10'] = viewable_impressions_delivered


        if table_data['Items']:
            row['generic1'] = creativeset_clicks_delivered - table_data['Items'][0].get(
                'creativeset_clicks_delivered_value')
            row['generic2'] = creativeset_impressions_delivered - table_data['Items'][0].get(
                'creativeset_impressions_delivered_value')
            row['generic3'] = creativeset_video_completions_delivered - table_data['Items'][0].get(
                'creativeset_video_completions_delivered_value')
            row['generic4'] = creativeset_video_starts_delivered - table_data['Items'][0].get(
                'creativeset_video_starts_delivered_value')
            row['generic5'] = creativeset_viewable_impressions_delivered - table_data['Items'][0].get(
                'creativeset_viewable_impressions_delivered_value')
            row['generic6'] = clicks_delivered - table_data['Items'][0].get('clicks_delivered_value')
            row['generic7'] = impressions_delivered - table_data['Items'][0].get('impressions_delivered_value')
            row['generic8'] = video_completions_delivered - table_data['Items'][0].get(
                'video_completions_delivered_value')
            row['generic9'] = video_starts_delivered - table_data['Items'][0].get('video_starts_delivered_value')
            row['generic10'] = viewable_impressions_delivered - table_data['Items'][0].get(
                'viewable_impressions_delivered_value')

        response = table.update_item(
            Key={
                "_id": id,
                "reference_id": reference_id
            },
            UpdateExpression=" set creativeset_clicks_delivered_value = :ccd," \
                             " creativeset_impressions_delivered_value =:cid," \
                             " creativeset_video_completions_delivered_value =:cvc," \
                             " creativeset_video_starts_delivered_value = :cvs,"
                             " creativeset_viewable_impressions_delivered_value = :cvi," \
                             " clicks_delivered_value =:cd," \
                             " impressions_delivered_value =:id,"\
                             " video_completions_delivered_value = :vc," \
                             " video_starts_delivered_value =:vs," \
                             " viewable_impressions_delivered_value =:vi,"
                             " time_stamp = :ts",
            ExpressionAttributeValues = {
                ':ccd': creativeset_clicks_delivered,
                ':cid': creativeset_impressions_delivered,
                ':cvc': creativeset_video_completions_delivered,
                ':cvs': creativeset_video_starts_delivered,
                ':cvi': creativeset_viewable_impressions_delivered,
                ':cd': clicks_delivered,
                ':id': impressions_delivered,
                ':vc': video_completions_delivered,
                ':vs': video_starts_delivered,
                ':vi': viewable_impressions_delivered,
                ':ts': ts
                })
        logger.info("Record updated in table for {}".format(alias))
        return row

    if alias == 'order':
        id = row['order_id']
        reference_id = alias
        table_data = table.query(
            KeyConditionExpression=Key('_id').eq(id) & Key('reference_id').eq(reference_id))
        row['generic1'] = row['total_clicks_delivered']
        row['generic2'] = row['total_impressions_delivered']
        row['generic3'] = row['total_viewable_impressions_delivered']
        if table_data['Items']:
            row['generic1'] = row['total_clicks_delivered'] - table_data['Items'][0].get('total_clicks_delivered_value')
            row['generic2'] = row['total_impressions_delivered'] - table_data['Items'][0].get(
                'total_impressions_delivered_value')
            row['generic3'] = row['total_viewable_impressions_delivered'] - table_data['Items'][0].get(
                'total_viewable_impressions_delivered_value')

        response = table.update_item(
        Key = {
            "_id": id,
            "reference_id": reference_id
        },
        UpdateExpression = " set total_clicks_delivered_value = :tc," \
                           " total_impressions_delivered_value =:ti," \
                           " total_viewable_impressions_delivered_value = :tvi,"
                           " time_stamp = :ts",
        ExpressionAttributeValues = {
            ':tc': row['total_clicks_delivered'],
            ':ti': row['total_impressions_delivered'],
            ':tvi': row['total_viewable_impressions_delivered'],
            ':ts': ts
        })
        logger.info("Record updated in table for {}".format(alias))
        return row

    return row

def deep_get(dictionary, *keys):
    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else [], keys, dictionary)

def process(input_bucket, input_key, output_bucket,
            date, alias, transformation_rules, transformation_columns, item, frequency_type, impression_table_name,
            yaml_path):
    try:
        output_path = input_key.replace('/raw/',
                                        '/transformation/').replace('json',
                                                                    'csv')
        s3_obj_res = get_s3_object(s3_key=input_key, bucket_name=input_bucket)
        if s3_obj_res['ContentLength'] == 0:
            logger.info('Blank object response for {0}'.format(input_key))
            return
        file_content = s3_obj_res['Body'].read().decode('utf-8')
        df = pd.read_json(StringIO(file_content))
        final_cols = [final_col['final_column_name'] for final_col in
                      transformation_rules]
        df_new = pd.DataFrame(columns=final_cols)
        for _data in transformation_rules:
            if _data['type'] == 'dict':
                df[_data['old_column_name']].fillna('{}', inplace=True)
                temp_col_df = json_normalize(df[_data[
                    'old_column_name']].apply(
                    only_dict).tolist()).add_prefix('{}_'.format(_data[
                                                                     'old_column_name']))
                if (alias == 'line_item_creative' and _data['old_column_name'] == 'endDateTime') or \
                        (alias == 'line_item_creative' and _data['old_column_name'] == 'startDateTime'):
                    temp_col_df['{}_date'.format(_data['old_column_name'])].fillna('{}', inplace=True)
                    temp_col_df_1 = json_normalize(temp_col_df['{}_date'.format(_data['old_column_name'])].apply(
                        only_dict).tolist()).add_prefix('{}'.format('{}_date'.format(_data['old_column_name'])))
                    temp_col_df = pd.concat([temp_col_df, temp_col_df_1], axis=1)
                temp_cols = temp_col_df.columns.tolist()
                if _data['mapping_column_name'] in temp_cols:
                    df_new[_data['final_column_name']] = temp_col_df[_data[
                        'mapping_column_name']].tolist()
                else:
                    df_new[_data['final_column_name']] = None
            else:
                df_new[_data['final_column_name']] = df[_data[
                    'old_column_name']].tolist()

        #For ad_unit, creative and line_item_creative api reports
        if alias == 'ad_units' or alias == 'creative' or alias == 'line_item_creative':
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
            export_format = 'CSV_DUMP'
            report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False,dir='./')
            report_downloader.DownloadReportToFile(
                report_job_id, export_format, report_file)

            report_file.close()
            df = pd.read_csv(report_file.name, compression='gzip', error_bad_lines=False)
            df_fixed = df.replace('-', np.nan)
            df_fixed.columns = item.get('report_job_columns')
            df_trans = add_generic_and_timestamp(df_fixed, date, frequency_type)
            df_type_reformat = change_float_columns_to_int(df_trans)
            df_expand_csv = df_type_reformat.to_csv(index=False, sep='|')
            expand_output_path = output_path.replace('ad_units',
                                                     'ad_units_report').replace \
                ('/creative/', '/creative_report/').replace('creative.csv','creative_report.csv') \
                .replace('line_item_creative', 'line_item_creative_ad_units_report')
            write_file_to_s3(df_expand_csv, output_bucket, expand_output_path,
                             content_type='text/csv')
            logger.info('File written at {}'.format(expand_output_path))
            os.remove(report_file.name)
            logger.info('file removed for {}'.format(alias))

        # For Targeting Location column in line item

        if alias == 'line_item':
            expand_columns = item.get('expand_columns_targeting_location')
            df_expand = df_new.copy()
            df_expand['targeting_location'] = df_new['targeting'].apply(
                        lambda x: deep_get(x,'geoTargeting','targetedLocations'))
            df_expand = extend_columns_in_rows(df_expand, 'targeting_location')
            df_expand['location_id'] = df_expand[
                'targeting_location'].apply(lambda x: x.get('id',None))
            df_expand['location_type'] = df_expand[
                'targeting_location'].apply(lambda x: x.get('type', None))
            df_expand['canonical_parent_id'] = df_expand[
                'targeting_location'].apply(lambda x: x.get('canonicalParentId', None))
            df_expand['location_name'] = df_expand[
                'targeting_location'].apply(lambda x: x.get('displayName', None))

            df_expand = df_expand[expand_columns]
            df_trans = add_generic_and_timestamp(df_expand, date, frequency_type)
            df_type_reformat = change_float_columns_to_int(df_trans)
            df_expand_csv = df_type_reformat.to_csv(index=False, sep='|')
            expand_output_path = output_path.replace('/line_item/', '/line_item_targeting_locations/') \
                .replace('line_item.csv', 'line_item_targeting_locations.csv')
            write_file_to_s3(df_expand_csv, output_bucket, expand_output_path,
                             content_type='text/csv')

        # For targetted columns in line_item and proposal_line_item
        if 'proposal_line_item' == alias or "line_item" == alias:
            expand_columns = item.get('expand_columns')
            df_expand = df_new.copy()
            df_expand['targeting_ad_include'] = df_new['targeting'].apply(
                lambda x: x['inventoryTargeting']['targetedAdUnits'])
            df_expand = extend_columns_in_rows(df_expand, 'targeting_ad_include')
            df_expand['adunit_id'] = df_expand[
                'targeting_ad_include'].apply(lambda x: x.get('adUnitId',
                                                              None))
            df_expand['include_descendants'] = df_expand[
                'targeting_ad_include'].apply(lambda x: x.get('includeDescendants',
                                                              None))
            df_expand['targetting_type'] = 'targetted'
            df_expand = df_expand[expand_columns]
            df_trans = add_generic_and_timestamp(df_expand, date, frequency_type)
            df_type_reformat = change_float_columns_to_int(df_trans)
            df_expand_csv = df_type_reformat.to_csv(index=False, sep='|')
            expand_output_path = output_path.replace('proposal_line_item',
                                                     'proposal_line_item_targetted_ad_unit').replace \
                ('/line_item/', '/line_item_targetted_ad_unit/') \
                .replace('line_item.csv', 'line_item_targetted_ad_unit.csv')
            write_file_to_s3(df_expand_csv, output_bucket, expand_output_path,
                             content_type='text/csv')

        # for custom_field column in line_item
        if "line_item" == alias:
            expand_columns = item.get('expand_columns_custom_field')
            df_expand = df_new.copy()
            df_expand = extend_columns_in_rows(df_expand, 'customFieldValues')
            df_expand['custom_field_id'] = df_expand['customFieldValues'].apply(lambda x: x.get('customFieldId',
                                                                                                None))
            df_expand['custom_field_value'] = df_expand['customFieldValues'].apply(lambda x: x.get('value', None)
                                                                                   .get('value', None) if x.get('value',
                                                                                        None) is not None else None)
            df_expand = df_expand[expand_columns]
            df_trans = add_generic_and_timestamp(df_expand, date, frequency_type)
            df_type_reformat = change_float_columns_to_int(df_trans)
            df_expand_csv = df_type_reformat.to_csv(index=False, sep='|')
            expand_output_path = output_path.replace \
                ('/line_item/', '/line_item_custom_field/') \
                .replace('line_item.csv', 'line_item_custom_field.csv')
            write_file_to_s3(df_expand_csv, output_bucket, expand_output_path)

        # Label Service Type fixed
        if "label" == alias:
            df_new['type'] = df_new['types'].apply(lambda x: x[0])

        df_transformed = df_new[transformation_columns]

        df_type_reformat = change_float_columns_to_int(df_transformed)

        # Applying StartDate filter on services
        for item in startdate_filter_services:
            if item == alias:
                df_type_reformat = df_type_reformat[df_type_reformat.start_datetime_date_year >= 2019]

        logger.info("Dataframe restructured for {0}".format(alias))
        df_trans = add_generic_and_timestamp(df_type_reformat, date, frequency_type)

        df_updated = df_trans.apply(lambda row: update_delivery_data(row, impression_table_name, alias), axis=1)
        if alias == 'line_item_creative':
            df_updated.insert(49, 'generic11', value=None, allow_duplicates=False)
        df_csv = df_updated.to_csv(sep='|', index=False)
        write_file_to_s3(df_csv, output_bucket, output_path,
                         content_type='text/csv')
        logger.info("successfully written to s3 path {0} ".format(output_path))
    except Exception as e:
        logger.exception(e)
    return


def main(*args):
    """
  Args 1: Output_Bucket
  Args 2: Table_name
  Args 3: Source_Name
  Args 4: Date
  Args 5: Network_ID
  Args 6: Input_Bucket
  Args 7: Pipeline_Time
  Args 8: Frequency Type
  Args 9: Impression Table Name
  Args 10: YAML File PAth
  """
    try:

        if len(args) < 10:
            logger.error(args)
            raise Exception("Please pass all the arguments in order "
                            "1. Output_Bucket 2. Table_name 3. Source_Name"
                            "4. Date 5. Network_ID 6. Input_Bucket 7. Pipeline Time")

        logger.info(args)
        output_bucket = args[0]
        table_name = args[1]
        source_name = args[2]
        date = datetime.strptime(args[3], '%Y/%m/%d')
        network_id = args[4]
        input_bucket = args[5]
        pipeline_time = args[6]
        frequency_type = args[7]
        impression_table_name = args[8]
        yaml_path = args[9]
        year = date.year
        month = date.month
        day = date.day
        monthh = '{:02}'.format(month)
        dayy = '{:02}'.format(day)
        schedule_type = os.environ.get('SCHEDULE_TYPE', 'scheduled')
        ad_manager_services = get_data_from_dynamodb(table_name, 'source',
                                                     source_name)
        for item in ad_manager_services['services']:
            alias = item['alias']
            transformation_rules = item.get('transformation_rules', None)
            transformation_columns = item['transformation_columns']
            input_key = "ad-manager/{}/{}/{}/{}/year={}/month={}/day={}/{" \
                        "}.json".format(alias, network_id, 'raw', 'json', year,
                                        monthh, dayy, alias)
            logger.info("Working on {}".format(alias))
            process(input_bucket, input_key, output_bucket, date, alias,
                    transformation_rules, transformation_columns, item, frequency_type, impression_table_name,yaml_path)

    except Exception as e:
        logger.exception(e)
        error_notify_v2(DATA_SOURCE, notification_type='PipelineFailure',
                        schedule_type=schedule_type,
                        _date=date,
                        notification_time=pipeline_time,
                        message=str(e))
