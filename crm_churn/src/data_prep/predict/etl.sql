select *

from app_olist.tb_seller_book

where nr_partition_year = year('{dt_ref}')
and nr_partition_month = month('{dt_ref}')
and nr_partition_day = day('{dt_ref}')