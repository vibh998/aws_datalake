import json
import boto3
import os
from logging_setup_etl import setup_logging
import sys
ERRORS = []
schedule_type = os.environ.get('SCHEDULE_TYPE', 'scheduled')
from ga_process.utils import (
    get_s3_object,
    error_notify_v2,
    copy_data_from_bucket_to_bucket,
    get_data_from_dynamodb,
    write_file_to_s3
)
from s3_to_s3.copy_process import (
    generate_path
)

from common_process.utils import (
    get_updated_path
)

reload(sys)
sys.setdefaultencoding('utf8')
logger = setup_logging('logging_conf.json')

DATA_SOURCE = 'ad_manager'
def get_keys_of_bucket(bucket_name, prefix_path=""):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    keys = bucket.objects.filter(Prefix=prefix_path)
    return [key.key for key in keys if "." in key.key]
def json_to_json(data):
    try:
        json_content = ''
        s3_obj_dict = data
        if type(s3_obj_dict) == list:
            for s3_str in s3_obj_dict:
                if json_content == '':
                    json_content = json.dumps(s3_str)
                else:
                    json_content += ',\n' + json.dumps(s3_str)
        else:
            json_content = json.dumps(s3_obj_dict) + '\n'
        return json_content
    except Exception as e:
        print(e)


def process(**kwargs):
    rules = get_data_from_dynamodb(kwargs.get('cleansing_table_name'),
                                   'source', kwargs.get('source_name'))
    try:
        table_name = kwargs.get('s3_input_path').split('/')[3]
        if 'csv' in kwargs.get('s3_input_path'):
            copy_data_from_bucket_to_bucket(kwargs.get('s3_input_path'), kwargs.get('input_bucket'), kwargs.get('s3_output_path'), kwargs.get('output_bucket'))
        else:
            s3_obj_res = get_s3_object(s3_key=kwargs.get('s3_input_path'),
                                       bucket_name=kwargs.get('input_bucket'))
            data = s3_obj_res['Body'].read()
            data = json.loads(data)
            services = rules['services']
            for record in services:
                if record['alias'] == table_name:
                    masking_cols = record['masking_cols']
                    if len(masking_cols) > 0:
                        for val in data:
                            for col in masking_cols:
                                del val[col]
            json_data = json_to_json(data)
            write_file_to_s3(json_data,kwargs['output_bucket'], kwargs.get('s3_output_path'))

        logger.info("successfully written to s3 path {0} ".format(kwargs.get('s3_output_path')))
    except Exception as e:
        logger.exception(e)
        ERRORS.append("Table {} has error : {}".format(table_name, str(e)))

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
                            "4. Date 5. Input_Bucket 6. Pipeline Time")

        logger.info(args)
        _kwargs = {
            "output_bucket": args[0],
            "source_folder_structure": args[1],
            "cleansing_table_name": args[2],
            "source_name": args[3],
            "date": args[4],
            "input_bucket": args[5],
            "pipeline_time": args[6]
        }
        schedule_type = os.environ.get('SCHEDULE_TYPE', 'scheduled')
        generated_paths = generate_path(_kwargs['input_bucket'], _kwargs['source_folder_structure'], _kwargs['date'])
        for source_key_list in generated_paths:
            key_list = get_keys_of_bucket(_kwargs['input_bucket'], source_key_list)
            for key in key_list:
                _kwargs['s3_input_path'] = str(key)

                s3_output_path = key.replace('raw', 'partitioning')
                _kwargs['s3_output_path'] = get_updated_path(s3_output_path)
                _kwargs['s3_output_error_path'] = key.replace('/raw/',
                                                              '/error/')
                process(**_kwargs)
        if len(ERRORS) > 0:
            error_notify_v2(DATA_SOURCE, notification_type='CompletedWithErrors',
                            schedule_type=schedule_type,
                            _date=_kwargs['date'],
                            notification_time=_kwargs['pipeline_time'],
                            message=ERRORS)



    except Exception as e:
        logger.exception(e)
        error_notify_v2(DATA_SOURCE, notification_type='PipelineFailure',
                        schedule_type=schedule_type,
                        _date=_kwargs['date'],
                        notification_time=_kwargs['pipeline_time'],
                        message=str(e))
