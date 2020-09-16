import os
import argparse
from pyspark.sql import SparkSession
import datetime

from dateutil.relativedelta import relativedelta

parser = argparse.ArgumentParser()
parser.add_argument("--exec", choices=['create', 'insert'], default='insert')
parser.add_argument("--date", type=str, default='2017-01-01')
parser.add_argument("--date_end", type=str, default=None)
parser.add_argument("--period", type=str, choices=['daily', 'monthly'])
args = parser.parse_args()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def import_query(path, **kwargs):
    with open( path, **kwargs ) as open_file:
        query = open_file.read()
    return query

def exec_queries(spark, query):
    for q in query.split(";")[:-1]:
        spark.sql( q )

def exec_insert(date, spark):
    # Importa query
    query = import_query(os.path.join(BASE_DIR, 'etl.sql'))
   
    # Formata a query para fazer inserção na data que foi passada
    insert = import_query(os.path.join(BASE_DIR, 'insert.sql'))
    query = query.format(dt_ref=date, insert_into="INSERT INTO app_olist.tb_seller_book")
    full_query = insert.format(query=query, dt_ref=date)

    # Executa a query
    exec_queries(spark, full_query)

def exec_many_dates(date_start:str, date_end:str):
    # Rranforma datas de texto para datetime
    exec_date = datetime.datetime.strptime(date_start,"%Y-%m-%d")
    date_end_datetime = datetime.datetime.strptime(date_end,"%Y-%m-%d")
    
    # Abre sessão com spark
    spark = SparkSession.builder.getOrCreate()
    
    # Faz loop até a data de execução ser maior que a ultima data
    while exec_date <= date_end_datetime:
        exec_date_str = exec_date.strftime("%Y-%m-%d")
        print("\n\n", exec_date_str,end="\n\n")
        exec_insert(exec_date_str, spark)
        if args.period == 'daily':
            exec_date += relativedelta(days=1)
        else:
            exec_date += relativedelta(months=1)

# Executa a criação da tabela a ser populada
if args.exec == 'create':
    query = import_query(os.path.join(BASE_DIR, 'etl.sql'))
    create = import_query(os.path.join(BASE_DIR, 'create.sql' ))
    query = query.format(dt_ref=args.date, insert_into="")
    full_query = create.format(query=query)
    spark = SparkSession.builder.getOrCreate()
    exec_queries(spark, full_query)

# Realiza o insert na tabela a ser populada
elif args.exec == 'insert':
    if args.date_end:
        exec_many_dates(args.date, args.date_end)
    else:
        spark = SparkSession.builder.getOrCreate()        
        exec_insert(args.date, spark)