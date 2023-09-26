# Databricks notebook source
# MAGIC %pip install great_expectations

# COMMAND ----------

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

#dbutils.fs.mkdirs("dbfs:/great_expectations/")

# COMMAND ----------

root_directory = "/dbfs/great_expectations"

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

my_spark_datasource_config = {
    "name": "sample-data-ource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "sample-data-ource": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            #"batch_identifiers":["batchID"],
          "batch_identifiers":[""],
        }
    },
}

# COMMAND ----------

context.test_yaml_config(yaml.dump(my_spark_datasource_config))
context.add_datasource(**my_spark_datasource_config)

# COMMAND ----------

####sample to be replaces by runtime micro batch

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("dbfs:/FileStore/tables/olist_orders_dataset_03052018.csv")

# COMMAND ----------

df

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="sample-data-ource",
    data_connector_name="sample-data-ource",
    data_asset_name="order_item_dataset",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
#         "some_key_maybe_pipeline_stage": "prod",
#         "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
          #"batchID": batchIDfromMicroBatch,
      "":""
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

expectation_suite_name = "sample_expectation"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column = "shipping_limit_date")

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "sample_checkpoint"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}

# COMMAND ----------

my_checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))

# COMMAND ----------

context.add_checkpoint(**checkpoint_config)

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# COMMAND ----------

checkpoint_result["run_results"][checkpoint_result.list_validation_result_identifiers()[0]]

# COMMAND ----------

import json

# def results(jsonin):
#     return jsonin["run_results"][list(jsonin["run_results"].keys())[0]]["validation_result"]["results"][0]

def results(jsonin):
    return jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]
  
def results_expectation(jsonin):
  print(len(jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"]))
  dict_results = {
    "column_name" : jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]['expectation_config']['kwargs']['column'],\
    "batch_id" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]['expectation_config']['kwargs']['batch_id'],\
    "expectation_type" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]['expectation_config']['expectation_type'],\
    "success" : jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]['success'],\
    "element_count" :   jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]["result"]["element_count"],\
    "unexpected_count" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]["result"]["unexpected_count"]
             }
  return dict_results


def success(resultsin):
    return resultsin["success"]

def elements(resultsin):
    return results(resultsin)["result"]["element_count"]

def errors(resultsin):
    return results(resultsin)["result"]["unexpected_count"]

# COMMAND ----------

print(results_expectation(checkpoint_result))
print(results(checkpoint_result))

print(success(checkpoint_result))
print(elements(checkpoint_result))
print(errors(checkpoint_result))


# COMMAND ----------

import json
ss=json.dumps(dd)

# COMMAND ----------

checkpoint_result["run_results"][checkpoint_result.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]

# COMMAND ----------

checkpoint_result.list_validation_result_identifiers()[0]

# COMMAND ----------

dict_results = checkpoint_result.to_raw_dict()

# COMMAND ----------

dict_results.keys()

# COMMAND ----------

sample_rdd = spark.sparkContext.parallelize(dict_results.values())

# COMMAND ----------

config_df = spark.sql(f"select * from streaming_dq_config").filter(f"Active = 'y' and process_id = 'pid01'").toPandas()
    #display(config_df)

config_dict = {x:eval(y) for x,y in zip(config_df.col_name,config_df.formatting_rules)}
    #print(config_dict)

key_set = {key for sub_keys in map(dict.values, config_dict.values()) for key in sub_keys}
    #print(key_sets)

dq_config = {key:[col for col in config_dict if key in config_dict[col].values()] for key in key_set }

# COMMAND ----------

eval(config_df['formatting_rules'][0]).values()

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/olist_orders_dataset_09112022.csv")

# COMMAND ----------

for i in key_set:
  print(i)

# COMMAND ----------

#add microbatch id #add filename 

# COMMAND ----------

def invoke_great_expectations(batchIDfromMicroBatch  , batchDf):
  #b_id = batchDf.select("_filename").collect()[0][0]
  filename = batchDf.select("order_status").collect()[0][0]
  root_directory = "/dbfs/great_expectations"
  data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
      root_directory=root_directory
    ),
  )
  context = BaseDataContext(project_config=data_context_config)
  
  my_spark_datasource_config = {
    "name": "sample-data-source_1",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "sample-data-source_1": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers":["batchID"],
        }
    },
  }
  context.test_yaml_config(yaml.dump(my_spark_datasource_config))
  context.add_datasource(**my_spark_datasource_config)
  
  batch_request = RuntimeBatchRequest(
    datasource_name="sample-data-source_1",
    data_connector_name="sample-data-source_1",
    data_asset_name="order_dataset",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
#         "some_key_maybe_pipeline_stage": "prod",
#         "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
          "batchID": filename,
          #"":""
    },
    runtime_parameters={"batch_data": batchDf},  # Your dataframe goes here
  )
  expectation_suite_name = "sample_expectation_1"
  context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
  )
  
  #context.add_checkpoint(**checkpoint_config)
  validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
  )
  
  ##generate dynamic validations here
  validator.expect_column_values_to_not_be_null(column = "order_purchase_timestamp")
  validator.save_expectation_suite(discard_failed_expectations=False)
  checkpoint_name = "sample_checkpoint"
  
  checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
  }
  
  context.test_yaml_config(yaml.dump(checkpoint_config))
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
  jsonin = checkpoint_result
  length_val = len(jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"])
  list_result = []
  for i in range(length_val):
    
    dict_results = {
     "file_identifier" : jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["meta"]["active_batch_definition"]["batch_identifiers"]["batchID"],\
    "batchID" : batchIDfromMicroBatch,\
    "column_name" : jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["results"][i]['expectation_config']['kwargs'] ['column'],\
    "great_exp_id" :jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
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
    
  df_expecation_result = spark.createDataFrame(list_result)
  df_expecation_result.write.mode("append").option("mergeSchema", "true").saveAsTable("great_expectations_results") 
    
  return jsonin
  
  
  
  
  
  

# COMMAND ----------

jsonin = invoke_great_expectations(1, df)

# COMMAND ----------

jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]\
      ["validation_result"]["meta"]["active_batch_definition"]["batch_identifiers"]["vvv"]

# COMMAND ----------

jsonin

# COMMAND ----------

df.select("order_status").collect()[0][0]

# COMMAND ----------

