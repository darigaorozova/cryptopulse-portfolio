-- Месячная витрина в dbt (ФТ-8): тот же расчёт, что dm_monthly_country_plan в DAG,
-- но для всех месяцев сразу и через ref() на модели над слоем ods.
-- Зерно: месяц × страна × тариф. Определения метрик — docs/data_model.md.

with trades as (
    select date_trunc('month', t.trade_date)::date as month,
           u.country_code, u.plan, t.user_id, t.amount_eur, t.fee_eur
    from {{ ref('v_trades') }} t
    join {{ ref('v_users') }} u using (user_id)
),

agg as (
    select month, country_code, plan,
           count(distinct user_id) as active_traders,
           count(*)                as completed_cnt,
           sum(amount_eur)         as volume_eur,
           sum(fee_eur)            as fee_revenue_eur
    from trades
    group by month, country_code, plan
),

verified as (
    select country_code, plan, count(*) as verified_users
    from {{ ref('v_users') }}
    where kyc_status = 'verified'
    group by country_code, plan
),

btc as (
    select date_trunc('month', price_date)::date as month, avg(price_eur) as avg_btc_price_eur
    from {{ ref('v_btc_prices') }}
    group by 1
)

select
    a.month || '|' || a.country_code || '|' || a.plan             as month_country_plan_key,
    a.month,
    a.country_code,
    a.plan,
    a.active_traders,
    coalesce(v.verified_users, 0)                                  as verified_users,
    round(a.active_traders::numeric / nullif(v.verified_users, 0), 4) as conversion_to_trade,
    a.completed_cnt,
    a.volume_eur,
    a.fee_revenue_eur,
    round(a.fee_revenue_eur / nullif(a.volume_eur, 0), 6)          as take_rate,
    b.avg_btc_price_eur
from agg a
left join verified v using (country_code, plan)
left join btc b using (month)
