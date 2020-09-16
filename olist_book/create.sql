DROP TABLE IF EXISTS app_olist.tb_seller_book;
CREATE TABLE app_olist.tb_seller_book
USING DELTA
OPTIONS(PATH='/mnt/app/olist/tb_sellers_book')
PARTITIONED BY (nr_partition_year, nr_partition_month, nr_partition_day)
AS
{query}