import pandas as pd
import os
import datetime
from pyspark.sql import SparkSession

ML_PREDICT_DIR = os.path.dirname(os.path.abspath(__file__))
ML_DIR = os.path.dirname(ML_PREDICT_DIR)
SRC_DIR = os.path.dirname(ML_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_PREDICT_DIR = os.path.join(DATA_DIR, 'predict')
DATA_SCORE_DIR = os.path.join(DATA_DIR, 'score')
MODEL_DIR = os.path.join(BASE_DIR, 'models')

print("Imporando os dados escorados...", end="")
df = pd.read_csv( os.path.join(DATA_SCORE_DIR, 'tb_score.csv'),
                  sep="|" )
print("Ok.")

print("Abrindo uma conexão com o Databrick...", end="")
spark = SparkSession.builder.getOrCreate()
print("Ok.")

print("Registrando TempTable...", end="")
sdf = spark.createDataFrame(df)
sdf.registerTempTable("tb_tmp_score_churn")
print("Ok.")

print("Realizando a inserção dos dados no Datalake...", end="")
try:
    with open( os.path.join(ML_PREDICT_DIR, 'insert.sql'), 'r' ) as open_file:
        query = open_file.read()
    for q in query.split(";")[:-1]:
        spark.sql(q)
except:
    with open( os.path.join(ML_PREDICT_DIR, 'create.sql'), 'r' ) as open_file:
        query = open_file.read()
    for q in query.split(";")[:-1]:
        spark.sql(q)
print("Ok.")