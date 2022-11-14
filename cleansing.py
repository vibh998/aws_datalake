import json
import pandas as pd
from datetime import datetime
import os
from flatten_json import flatten
from logging_setup_etl import setup_logging
import sys
from ga_process.utils import (
    get_s3_object,
    write_file_to_s3,
    error_notify_v2,
    get_data_from_dynamodb
)

reload(sys)
sys.setdefaultencoding('utf8')
logger = setup_logging('logging_conf.json')

DATA_SOURCE = 'ad_manager'


def process(input_bucket, input_key, output_bucket):
    try:
        output_path = input_key.replace('/raw/',
                                        '/cleansing/').replace('json', 'csv')
        s3_obj_res = get_s3_object(s3_key=input_key, bucket_name=input_bucket)
        if s3_obj_res['ContentLength'] == 0:
            logger.info('Blank object response for {0}'.format(input_key))
            return
        file_content = s3_obj_res['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        dic_flattened = (flatten(d) for d in json_content)
        df = pd.DataFrame(dic_flattened)
        df_csv = df.to_csv(sep='|', index=False)
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
  """
    try:

        if len(args) < 7:
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
        year = date.year
        month = date.month
        day = date.day
        monthh = '{:02}'.format(month)
        dayy = '{:02}'.format(day)
        schedule_type = os.environ.get('SCHEDULE_TYPE', 'scheduled')
        pipeline_time = datetime.now()
        ad_manager_services = get_data_from_dynamodb(table_name, 'source',
                                                     source_name)
        for item in ad_manager_services['services']:
            logger.info(item)
            alias = item['alias']
            input_key = "ad-manager/{}/{}/{}/{}/year={}/month={}/day={}/{" \
                        "}.json".format(
                alias, network_id, 'raw', 'json', year, monthh, dayy, alias)
            process(input_bucket, input_key, output_bucket)

    except Exception as e:
        logger.exception(e)
        error_notify_v2(DATA_SOURCE, notification_type='PipelineFailure',
                        schedule_type=schedule_type,
                        _date=date,
                        notification_time=pipeline_time,
                        message=str(e))
