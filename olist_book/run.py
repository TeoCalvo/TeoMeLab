import os
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--exec", choices=['create', 'insert'], default='insert')
parser.add_argument("--date", type=str, default='2017-01-01')
parser.add_argument("--date_end", type=str, default='2018-01-01')
args = parser.parse_args()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def import_query(path, **kwargs):
    with open( path, **kwargs ) as open_file:
        query = open_file.read()
    return query

def exec_queries(spark, query):
    for q in query.split(";")[:-1]:
        spark.sql( q )

# importa a query para o ETL
query = import_query(os.path.join(BASE_DIR, 'etl.sql'))

# Executa a criação da tabela a ser populada
if args.exec == 'create':
    create = import_query(os.path.join(BASE_DIR, 'create.sql' ))
    query = query.format(dt_ref=args.date, insert_into="")
    full_query = create.format(query=query)

# Realiza o insert na tabela a ser populada
elif args.exec == 'insert':
    insert = import_query(os.path.join(BASE_DIR, 'insert.sql'))
    query = query.format(dt_ref=args.date, insert_into="INSERT INTO app_olist.tb_seller_book")
    full_query = insert.format(query=query, dt_ref=args.date)

spark = SparkSession.builder.getOrCreate()
exec_queries(spark, full_query)