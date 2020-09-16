with tb_venda as ( -- View para pegar transações menores do que nossa data de referencia
  select *
  from olist.tb_orders as t1
  where t1.order_approved_at < '{dt_ref}'
  and t1.order_status = 'delivered'
),

tb_seller_vida as ( -- view para pegar informações da vida do seller antes da data de referencia

  select t2.seller_id,
          min(t1.order_approved_at) as seller_prim_venda, -- data da primeira venda no Olist
          sum(t2.price) as seller_receita_vida, -- receita total da vida no Olist
          count( distinct t2.order_id) as seller_qt_pedidos_vida, -- qtde de pedidos na vida no Olist
          count( distinct t2.product_id ) as seller_qt_item_dist_vida -- quantidade de itens distintos vendidos pelo Olist

  from tb_venda as t1 -- tabela de vendas com os dados até a data de referencia

  left join olist.tb_order_items as t2 -- tabela de itens/pedido
  on t1.order_id = t2.order_id

  group by t2.seller_id
),

tb_seller_ciclo as (

  select t2.seller_id,
          sum(t2.price) as seller_receita_ciclo, -- receita de venda no olist no ciclo de 3 meses
          count(distinct t1.order_id) as sellet_qt_pedidos_ciclo, -- qtde de pedidos realizados no cilo
          sum(t2.price) / count(distinct t1.order_id) as seller_ticket_medio_ciclo, -- ticket médio no ciclo
          count(*) as seller_qt_item_ciclo, -- qtde de itens vendidos no ciclo 
          count( distinct t2.product_id ) as seller_qt_item_dist_ciclo, -- qtde de itens distintos vendidos no ciclo
          min(datediff('{dt_ref}',order_approved_at)) as seller_recencia_ciclo, -- quantidade de dia desde a ultima venda

          avg( datediff( order_delivered_customer_date, order_approved_at ) ) as seller_tempo_medio_entrega, -- média de tempo para entrega
          count(distinct month(order_approved_at)) as seller_qt_meses_ativacao_ciclo, -- quantidade de meses que houve venda
          count(distinct date(order_approved_at)) as seller_qt_dias_ativacao_ciclo, -- qnde de dias que houve venda

          /* Quebra de vendas por estado */
          count( case when T3.customer_state = 'AC' then t1.order_id else null end ) as seller_qt_venda_AC,
          count( case when T3.customer_state = 'AL' then t1.order_id else null end ) as seller_qt_venda_AL,
          count( case when T3.customer_state = 'AM' then t1.order_id else null end ) as seller_qt_venda_AM,
          count( case when T3.customer_state = 'AP' then t1.order_id else null end ) as seller_qt_venda_AP,
          count( case when T3.customer_state = 'BA' then t1.order_id else null end ) as seller_qt_venda_BA,
          count( case when T3.customer_state = 'CE' then t1.order_id else null end ) as seller_qt_venda_CE,
          count( case when T3.customer_state = 'DF' then t1.order_id else null end ) as seller_qt_venda_DF,
          count( case when T3.customer_state = 'ES' then t1.order_id else null end ) as seller_qt_venda_ES,
          count( case when T3.customer_state = 'GO' then t1.order_id else null end ) as seller_qt_venda_GO,
          count( case when T3.customer_state = 'MA' then t1.order_id else null end ) as seller_qt_venda_MA,
          count( case when T3.customer_state = 'MG' then t1.order_id else null end ) as seller_qt_venda_MG,
          count( case when T3.customer_state = 'MS' then t1.order_id else null end ) as seller_qt_venda_MS,
          count( case when T3.customer_state = 'MT' then t1.order_id else null end ) as seller_qt_venda_MT,
          count( case when T3.customer_state = 'PA' then t1.order_id else null end ) as seller_qt_venda_PA,
          count( case when T3.customer_state = 'PB' then t1.order_id else null end ) as seller_qt_venda_PB,
          count( case when T3.customer_state = 'PE' then t1.order_id else null end ) as seller_qt_venda_PE,
          count( case when T3.customer_state = 'PI' then t1.order_id else null end ) as seller_qt_venda_PI,
          count( case when T3.customer_state = 'PR' then t1.order_id else null end ) as seller_qt_venda_PR,
          count( case when T3.customer_state = 'RJ' then t1.order_id else null end ) as seller_qt_venda_RJ,
          count( case when T3.customer_state = 'RN' then t1.order_id else null end ) as seller_qt_venda_RN,
          count( case when T3.customer_state = 'RO' then t1.order_id else null end ) as seller_qt_venda_RO,
          count( case when T3.customer_state = 'RR' then t1.order_id else null end ) as seller_qt_venda_RR,
          count( case when T3.customer_state = 'RS' then t1.order_id else null end ) as seller_qt_venda_RS,
          count( case when T3.customer_state = 'SC' then t1.order_id else null end ) as seller_qt_venda_SC,
          count( case when T3.customer_state = 'SE' then t1.order_id else null end ) as seller_qt_venda_SE,
          count( case when T3.customer_state = 'SP' then t1.order_id else null end ) as seller_qt_venda_SP,
          count( case when T3.customer_state = 'TO' then t1.order_id else null end ) as seller_qt_venda_TO,

          /* quebra de valor de venda por categoria de produto */
          sum( case when t4.product_category_name in ('agro_industria_e_comercio') then t2.price else 0 end ) as seller_vl_agro_industria_e_comercio_ciclo,
          sum( case when t4.product_category_name in ('alimentos', 'alimentos_bebidas', 'bebidas') then t2.price else 0 end ) as seller_vl_alimentos_bebidas_ciclo,
          sum( case when t4.product_category_name in ('artes', 'artes_e_artesanato') then t2.price else 0 end ) as seller_vl_artes_ciclo,
          sum( case when t4.product_category_name in ('artigos_de_festas', 'artigos_de_natal') then t2.price else 0 end ) as seller_vl_artigos_de_festas_ciclo,
          sum( case when t4.product_category_name in ('audio') then t2.price else 0 end ) as seller_vl_audio_ciclo,
          sum( case when t4.product_category_name in ('automotivo') then t2.price else 0 end ) as seller_vl_automotivo_ciclo,
          sum( case when t4.product_category_name in ('bebes', 'fraldas_higiene') then t2.price else 0 end ) as seller_vl_bebes_ciclo,
          sum( case when t4.product_category_name in ('beleza_saude') then t2.price else 0 end ) as seller_vl_beleza_saude_ciclo,
          sum( case when t4.product_category_name in ('brinquedos') then t2.price else 0 end ) as seller_vl_brinquedos_ciclo,
          sum( case when t4.product_category_name in ('cama_mesa_banho') then t2.price else 0 end ) as seller_vl_cama_mesa_banho_ciclo,
          sum( case when t4.product_category_name in ('casa_conforto', 'casa_conforto_2', 'casa_construcao') then t2.price else 0 end ) as seller_vl_casa_ciclo,
          sum( case when t4.product_category_name in ('cds_dvds_musicais', 'dvds_blu_ray') then t2.price else 0 end ) as seller_vl_cds_dvds_musicais_ciclo,
          sum( case when t4.product_category_name in ('cine_foto') then t2.price else 0 end ) as seller_vl_cine_foto_ciclo,
          sum( case when t4.product_category_name in ('climatizacao') then t2.price else 0 end ) as seller_vl_climatizacao_ciclo,
          sum( case when t4.product_category_name in ('construcao_ferramentas_construcao', 'construcao_ferramentas_ferramentas', 'construcao_ferramentas_iluminacao', 'construcao_ferramentas_jardim', 'construcao_ferramentas_seguranca') then t2.price else 0 end ) as seller_vl_construcao_ciclo,
          sum( case when t4.product_category_name in ('cool_stuff') then t2.price else 0 end ) as seller_vl_cool_stuff_ciclo,
          sum( case when t4.product_category_name in ('eletrodomesticos', 'eletrodomesticos_2') then t2.price else 0 end ) as seller_vl_eletrodomesticos_ciclo,
          sum( case when t4.product_category_name in ('eletronicos') then t2.price else 0 end ) as seller_vl_eletronicos_ciclo,
          sum( case when t4.product_category_name in ('eletroportateis') then t2.price else 0 end ) as seller_vl_eletroportateis_ciclo,
          sum( case when t4.product_category_name in ('esporte_lazer') then t2.price else 0 end ) as seller_vl_esporte_lazer_ciclo,
          sum( case when t4.product_category_name in ('fashion_bolsas_e_acessorios', 'fashion_calcados', 'fashion_esporte', 'fashion_roupa_feminina', 'fashion_roupa_infanto_juvenil', 'fashion_roupa_masculina', 'fashion_underwear_e_moda_praia') then t2.price else 0 end ) as seller_vl_fashion_ciclo,
          sum( case when t4.product_category_name in ('ferramentas_jardim') then t2.price else 0 end ) as seller_vl_ferramentas_jardim_ciclo,
          sum( case when t4.product_category_name in ('flores') then t2.price else 0 end ) as seller_vl_flores_ciclo,
          sum( case when t4.product_category_name in ('industria_comercio_e_negocios') then t2.price else 0 end ) as seller_vl_industria_comercio_e_negocios_ciclo,
          sum( case when t4.product_category_name in ('instrumentos_musicais') then t2.price else 0 end ) as seller_vl_instrumentos_musicais_ciclo,
          sum( case when t4.product_category_name in ('la_cuisine') then t2.price else 0 end ) as seller_vl_la_cuisine_ciclo,
          sum( case when t4.product_category_name in ('livros_importados', 'livros_interesse_geral', 'livros_tecnicos') then t2.price else 0 end ) as seller_vl_livros_ciclo,
          sum( case when t4.product_category_name in ('malas_acessorios') then t2.price else 0 end ) as seller_vl_malas_acessorios_ciclo,
          sum( case when t4.product_category_name in ('market_place') then t2.price else 0 end ) as seller_vl_market_place_ciclo,
          sum( case when t4.product_category_name in ('moveis_colchao_e_estofado', 'moveis_cozinha_area_de_servico_jantar_e_jardim', 'moveis_decoracao', 'moveis_escritorio', 'moveis_quarto', 'moveis_sala') then t2.price else 0 end ) as seller_vl_moveis_ciclo,
          sum( case when t4.product_category_name in ('musica') then t2.price else 0 end ) as seller_vl_musica_ciclo,
          sum( case when t4.product_category_name in ('papelaria') then t2.price else 0 end ) as seller_vl_papelaria_ciclo,
          sum( case when t4.product_category_name in ('consoles_games','informatica_acessorios', 'pcs', 'tablets_impressao_imagem', 'pc_gamer') then t2.price else 0 end ) as seller_vl_informatica_ciclo,
          sum( case when t4.product_category_name in ('perfumaria') then t2.price else 0 end ) as seller_vl_perfumaria_ciclo,
          sum( case when t4.product_category_name in ('pet_shop') then t2.price else 0 end ) as seller_vl_pet_shop_ciclo,
          sum( case when t4.product_category_name in ('portateis_casa_forno_e_cafe', 'portateis_cozinha_e_preparadores_de_alimentos') then t2.price else 0 end ) as seller_vl_portateis_ciclo,
          sum( case when t4.product_category_name in ('relogios_presentes') then t2.price else 0 end ) as seller_vl_relogios_presentes_ciclo,
          sum( case when t4.product_category_name in ('seguros_e_servicos') then t2.price else 0 end ) as seller_vl_seguros_e_servicos_ciclo,
          sum( case when t4.product_category_name in ('sinalizacao_e_seguranca') then t2.price else 0 end ) as seller_vl_sinalizacao_e_seguranca_ciclo,
          sum( case when t4.product_category_name in ('telefonia', 'telefonia_fixa') then t2.price else 0 end ) as seller_vl_telefonia_ciclo,
          sum( case when t4.product_category_name in ('utilidades_domesticas') then t2.price else 0 end ) as seller_vl_utilidades_domesticas_ciclo,

          /* quebra de valor de meiod e pagemento */
          sum( case when payment_type = 'boleto' then t2.price else 0 end)  as seller_valor_pgmt_boleto_ciclo,
          sum( case when payment_type = 'credit_card' then t2.price else 0 end)  as seller_valor_pgmt_credit_card_ciclo,
          sum( case when payment_type = 'debit_card' then t2.price else 0 end)  as seller_valor_pgmt_debit_card_ciclo,
          sum( case when payment_type = 'not_defined' then t2.price else 0 end)  as seller_valor_pgmt_not_defined_ciclo,
          sum( case when payment_type = 'voucher' then t2.price else 0 end)  as seller_valor_pgmt_voucher_ciclo

  from tb_venda as t1 -- tabela de venda 

  left join olist.tb_order_items as t2 -- tabela de item/pedido
  on t1.order_id = t2.order_id

  left join olist.tb_customers as t3 -- tabela de consumidores
  on t1.customer_id = t3.customer_id

  left join olist.tb_products as t4 -- tabela de procutos
  on t2.product_id = t4.product_id

  left join olist.tb_order_payments as t5 -- tabela de meio de pagamento
  on t1.order_id = t5.order_id

  where t1.order_approved_at >= add_months('{dt_ref}', -3)
  group by t2.seller_id

)

{insert_into}
select  
        year('{dt_ref}') as nr_partition_year,
        month('{dt_ref}') as nr_partition_month,
        day('{dt_ref}') as nr_partition_day,
        '{dt_ref}' as dt_ref,
        t1.*,
        t1.seller_qt_dias_ativacao_ciclo / least( datediff('{dt_ref}', t2.seller_prim_venda), datediff('{dt_ref}', add_months('{dt_ref}', -3)) ) as seller_tx_ativacao_ciclo,
        t2.seller_receita_vida,
        datediff('{dt_ref}', t2.seller_prim_venda) as seller_idade_base,
        t2.seller_qt_pedidos_vida,
        t2.seller_qt_item_dist_vida

from tb_seller_ciclo as t1

left join tb_seller_vida as t2
on t1.seller_id = t2.seller_id;