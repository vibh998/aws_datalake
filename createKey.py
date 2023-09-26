# Databricks notebook source
cffi==1.15.1
cryptography==37.0.4
pycparser==2.21

# COMMAND ----------

!pip install --upgrade pip

# COMMAND ----------

!pip install pycparser==2.21

# COMMAND ----------

from cryptography.fernet import Fernet
import os

keyFileName = "key_1.data"

def generateKey():
    key = Fernet.generate_key()
    print(key)
    return key

def createKeyFile():
    if os.path.exists(keyFileName):
        print("key already exists")
        return
    else:
        print("creating key")
        key = generateKey()
        dbutils.fs.put("/tmp/"+keyFileName, str(key) , True)
        #outfile.write(key)



createKeyFile()

# COMMAND ----------

import os
import pathlib
import fnmatch
import pandas as pd
import boto3
import delta
import pyspark.sql.functions as F
from pyspark.sql.functions import input_file_name
import pyspark.sql.types as T
from pyspark.sql.types import StructType,StructField, StringType, IntegerType , DateType
from datetime import datetime
from delta.tables import DeltaTable

##import encryption libraries 

from cryptography.fernet import Fernet
import hashlib

# COMMAND ----------

def get_encryption_key(key_path):
    if (dbutils.fs.ls(key_path)):
      df = spark.read.format("text").load(key_path)
      df = df.withColumn("value", F.col("value").cast(T.StringType()))
      return (df)
    else :
      raise Exception ("keyfile not found")
    pass

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("dbfs:/FileStore/tables/olist_order_items_dataset_06082017.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

key = get_encryption_key('dbfs:/tmp/key_1.data')

broadcast_key = spark.sparkContext.broadcast(key.collect()[0][0].split("b'")[1].encode())

# COMMAND ----------

#key.collect()[0][0].split("b'")[1].encode()


broadcast_key.value

# COMMAND ----------


x= "b'gAAAAABjgKrDyVOq2mHWBjuMpSxKFAVM1Hv8c0OHYjy_Biyb4t6ceR-HarxdgTNB0XNsOlPAcgNZVKpl2iitL1Y5XntULQZ2UTgt633KZ-p7oltZUUJW4JWk0ZC5f_iJRJvuEe_5yN82'"


f = Fernet(broadcast_key.value) 
f.decrypt(x.split("b'")[1].encode())


# COMMAND ----------

def encrypt_a_val_1(x):
      col_bytes = str(x).encode()
      #f = Fernet(b'JjyE9jbfXSejAVWZMiHzh25CZ9eURiVYUkJBaJYIf44=')  broadcast_key.value
      f = Fernet(broadcast_key.value) 
      
      token = f.encrypt(col_bytes)
      return str(token)
    
    

    
encrypt_a_val_udf_1 = F.udf(encrypt_a_val_1 , T.StringType())
      


# COMMAND ----------

def decrypt_a_val_1(x):
      #key = get_encryption_key('dbfs:/tmp/key_1.data')  
      x_byte = x.split("b'")[1].encode()
      f = Fernet(broadcast_key.value)
      col_bytes = f.decrypt(x_byte)
      col_string=str(col_bytes)
      return col_string
    
decrypt_a_val_udf_1 = F.udf(decrypt_a_val_1 , T.StringType())
      


# COMMAND ----------

col_name ='order_id'
df_1 = df.withColumn(col_name+"_encrypted" , encrypt_a_val_udf_1(F.col(col_name)))
df_1 = df_1.withColumn(col_name+"_decrypted" , decrypt_a_val_udf_1(F.col(col_name+"_encrypted")))


# COMMAND ----------

display(df_1)

# COMMAND ----------

