DELETE FROM app_olist.tb_score_churn
WHERE dt_ref IN (SELECT DISTINCT DT_REF FROM tb_tmp_score_churn);

INSERT INTO app_olist.tb_score_churn
select year(t1.dt_ref) as nr_partition_year,
        month(t1.dt_ref) as nr_partition_month,
        day(t1.dt_ref) as nr_partition_day,
        t1.*
    
from tb_tmp_score_churn as t1;