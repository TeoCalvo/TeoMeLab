import pandas
import os
from pyspark.sql import SparkSession

DATA_PREP_TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PREP_DIR = os.path.dirname(DATA_PREP_TRAIN_DIR)
SRC_DIR = os.path.dirname(DATA_PREP_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join( BASE_DIR, 'data' )
DATA_TRAIN_DIR = os.path.join( DATA_DIR, 'train' )

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)

if not os.path.exists(DATA_TRAIN_DIR):
    os.mkdir(DATA_TRAIN_DIR)

print("Abrindo conexão com o spark...", end="")
spark = SparkSession.builder.getOrCreate()
print("ok.")

print("Coletando ABT do datalake...", end="")
df_abt = spark.table("app_olist.tb_abt_churn").toPandas()
df_abt.to_csv( os.path.join(DATA_DIR, 'train', 'abt_churn.csv'),
               index=False,
               sep="|" )
print("ok.")
