drop table if exists app_olist.tb_abt_churn;
create table app_olist.tb_abt_churn as
with tb_safras_ref (

  select *

  from app_olist.tb_seller_book

  where  nr_partition_year <= 2018
  and (nr_partition_day = 01 or nr_partition_day = 16 )
  and date_add( dt_ref,  91 - seller_recencia_ciclo) <= '2018-01-01' -- verifica se a observação está madura, penaliza obeservações
)

select t1.*,
        case when t2.seller_id is null then 1 else 0 end as flag_churn

from tb_safras_ref as t1

left join app_olist.tb_seller_book as t2
on t1.seller_id = t2.seller_id
and date_add( t1.dt_ref,  91 ) = t2.dt_ref;