# Databricks notebook source
import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

def invoke_great_expectations(batchIDfromMicroBatch  , batchDf):
  data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
      root_directory=root_directory
    ),
  )
  context = BaseDataContext(project_config=data_context_config)
  
  my_spark_datasource_config = {
    "name": "sample-data-ource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "sample-data-ource": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers":["batchID"],
        }
    },
  }
  batch_request = RuntimeBatchRequest(
    datasource_name="sample-data-ource",
    data_connector_name="sample-data-ource",
    data_asset_name="order_item_dataset",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
#         "some_key_maybe_pipeline_stage": "prod",
#         "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
          "batchID": batchIDfromMicroBatch,
    },
    runtime_parameters={"batch_data": batchDf},  # Your dataframe goes here
  )
  expectation_suite_name = "sample_expectation"
  context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
  )
  validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
  )
  
  ##generate dynamic validations here
  validator.expect_column_values_to_not_be_null(column = "shipping_limit_date")
  validator.save_expectation_suite(discard_failed_expectations=False)
  checkpoint_name = "sample_checkpoint"
  
  checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
  }
  
  context.add_checkpoint(**checkpoint_config)
  
  checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
  )
  
  #write checkpoint result into a dq_validation table -
  length_val = len(jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"])
  checkpoint_result = []
  for i in length_val-1:
    
    dict_results = {
    "column_name" : jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]['expectation_config']['kwargs'] ['column'],\
    "batch_id" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]['expectation_config']['kwargs']['batch_id'],\
    "expectation_type" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]['expectation_config']['expectation_type'],\
    "success" : jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]['success'],\
    "element_count" :   jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]["result"]["element_count"],\
    "unexpected_count" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]["result"]["unexpected_count"]
             }
    list_result.append(dict_results)
    
  df_expecation_result = spark.createDataFrame(checkpoint_result)
  df_expecation_result.write.mode("append").saveAsTable("great_expectations_results") 
    
  pass
  
