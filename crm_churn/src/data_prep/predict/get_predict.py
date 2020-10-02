import pandas
import os
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date", "-d", default='2018-01-01', type=str, help="Data referencia para realizar a escoragem")
args = parser.parse_args()

DATA_PREP_PRED_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PREP_DIR = os.path.dirname(DATA_PREP_PRED_DIR)
SRC_DIR = os.path.dirname(DATA_PREP_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join( BASE_DIR, 'data' )
DATA_TRAIN_DIR = os.path.join( DATA_DIR, 'predict' )

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)

if not os.path.exists(DATA_TRAIN_DIR):
    os.mkdir(DATA_TRAIN_DIR)

# Vamos importar query
print("Importando query...", end="")
with open( os.path.join(DATA_PREP_PRED_DIR, 'etl.sql'), 'r' ) as open_file:
    query = open_file.read()
query = query.format(dt_ref=args.date)
print("ok.")

print("Abrindo conexão com o spark...", end="")
spark = SparkSession.builder.getOrCreate()
print("ok.")

print("Coletando ABT do datalake...", end="")
df_predict = spark.sql(query).toPandas()
df_predict.to_csv(os.path.join(DATA_TRAIN_DIR, 'tb_predict.csv'),
                  index=False,
                  sep="|" )
print("ok.")
