# Databricks notebook source
import great_expectations as ge
from ruamel import yaml
context = ge.data_context.DataContext("/dbfs/great_expectations")
from great_expectations.core.batch import RuntimeBatchRequest


# COMMAND ----------

# DBTITLE 1,Load DF of table and DF of file to be ingested

df_original_table = spark.table("hive_metastore.andytests1.existing_reservation_csv")
df_newfile = spark.read.option("delimiter","\t").option("header","True") \
    .csv("dbfs:/example_data/example_data.csv")



# COMMAND ----------

# DBTITLE 1,Make a DF of the above union'ed (only taking reservationid - dupe check only for PoC)
df_check = df_original_table.select ("reservationid").union(df_newfile.select("reservationid"))


# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="general_spark_dataframe",
    data_connector_name="general_spark_dataframe_connector",
    batch_identifiers={"None": "None"},
    data_asset_name="reservation_table",
    runtime_parameters={"batch_data": df_check}
)

# COMMAND ----------

# MAGIC %md
# MAGIC # The next 3 commands are config and can be skipped on re-run

# COMMAND ----------

# DBTITLE 1,CONFIG: suite
context.create_expectation_suite(
    expectation_suite_name="df_table_file_dupecheck",
    overwrite_existing=True
)



# COMMAND ----------

# DBTITLE 1,CONFIG: validators
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="df_table_file_dupecheck",
)

validator.expect_column_values_to_be_unique("reservationid")

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

# DBTITLE 1,CONFIG: Checkpoint
config = """
name: table_file_dupecheck_checkpoint
config_version: 1
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: general_spark_dataframe
      data_connector_name: general_spark_dataframe_connector
      data_asset_name: reservation_table
    expectation_suite_name: df_table_file_dupecheck
"""

context.add_checkpoint(**yaml.load(config))

# COMMAND ----------

# DBTITLE 1,Run checkpoint for existing table - this time capture JSON results
result_table = context.run_checkpoint(
    checkpoint_name="table_file_dupecheck_checkpoint",
    batch_request={
        "runtime_parameters": {"batch_data": df_original_table},
        "batch_identifiers": {
            "None": "None"
        },
    },
)

print (result_table)

# COMMAND ----------

# DBTITLE 1,Run checkpoint for the file (only) and capture result
result_newfile = context.run_checkpoint(
    checkpoint_name="table_file_dupecheck_checkpoint",
    batch_request={
        "runtime_parameters": {"batch_data": df_newfile},
        "batch_identifiers": {
            "None": "None"
        },
    },
)

print (result_newfile)

# COMMAND ----------

# DBTITLE 1,Run checkpoint for reservationid union DF
result_merged = context.run_checkpoint(
    checkpoint_name="table_file_dupecheck_checkpoint",
    batch_request={
        "runtime_parameters": {"batch_data": df_check},
        "batch_identifiers": {
            "None": "None"
        },
    },
)

print (result_merged)

# COMMAND ----------

import json

# def results(jsonin):
#     return jsonin["run_results"][list(jsonin["run_results"].keys())[0]]["validation_result"]["results"][0]

def results(jsonin):
    return jsonin["run_results"][jsonin.list_validation_result_identifiers()[0]]["validation_result"]["results"][0]


def success(resultsin):
    return resultsin["success"]

def elements(resultsin):
    return results(resultsin)["result"]["element_count"]

def errors(resultsin):
    return results(resultsin)["result"]["unexpected_count"]




# COMMAND ----------

print(success(result_table))
print(success(result_newfile))
print(success(result_merged))
print('-----')
print(elements(result_table))
print(elements(result_newfile))
print(elements(result_merged))
print('-----')
print(errors(result_table))
print(errors(result_newfile))
print(errors(result_merged))


# COMMAND ----------

validation_result_identifier = result_merged.list_validation_result_identifiers()[0]
context.build_data_docs()

