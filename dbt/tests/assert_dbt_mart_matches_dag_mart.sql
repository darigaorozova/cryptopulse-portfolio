-- ФТ-8: результат dbt-модели совпадает с витриной, которую считает DAG final_orozova_datamart.
-- Тест возвращает строки-расхождения (месяц/страна/тариф, где отличаются трейдеры, объём
-- или комиссии) и падает, если они есть. Сравниваются только месяцы, посчитанные DAG.
with dag as (
    select month, country_code, plan, active_traders, volume_eur, fee_revenue_eur
    from orozova_datamart.dm_monthly_country_plan
),
dbt as (
    select month, country_code, plan, active_traders, volume_eur, fee_revenue_eur
    from {{ ref('dm_monthly_country_plan_dbt') }}
    where month in (select month from dag)
)
select coalesce(dbt.month, dag.month)               as month,
       coalesce(dbt.country_code, dag.country_code) as country_code,
       coalesce(dbt.plan, dag.plan)                 as plan
from dbt
full outer join dag using (month, country_code, plan)
where dbt.active_traders  is distinct from dag.active_traders
   or dbt.volume_eur      is distinct from dag.volume_eur
   or dbt.fee_revenue_eur is distinct from dag.fee_revenue_eur
