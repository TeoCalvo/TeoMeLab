import os
from pyspark.sql import SparkSession
from tqdm import tqdm
import time

TRAIN_DIR = os.path.join(os.path.abspath('.'), *'crm_churn/src/data_prep/train/'.split("/"))
TRAIN_DIR = os.path.dirname( os.path.abspath(__file__) )
DATA_PREP_DIR = os.path.dirname(TRAIN_DIR)
SRC_DIR = os.path.dirname(DATA_PREP_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)

# Lendo um arquivo de texto que possui a query
print("Importando query...", end="")
with open( os.path.join( TRAIN_DIR, 'etl.sql' ), 'r' ) as open_file:
    query = open_file.read()
print("ok.")

# Executa todas as queries do arquivo
print("Abrindo conexão com o banco...", end="")
spark = SparkSession.builder.getOrCreate()
print("ok.")

for q in tqdm(query.split(";")[:-1]):
    spark.sql(q)
