drop table if exists app_olist.tb_score_churn;
create table app_olist.tb_score_churn
USING DELTA
OPTIONS(PATH='/mnt/app/olist/tb_score_churn')
PARTITIONED BY (nr_partition_year, nr_partition_month, nr_partition_day)
AS (
    select year(t1.dt_ref) as nr_partition_year,
           month(t1.dt_ref) as nr_partition_month,
           day(t1.dt_ref) as nr_partition_day,
           t1.*
        
    from tb_tmp_score_churn as t1
)
;