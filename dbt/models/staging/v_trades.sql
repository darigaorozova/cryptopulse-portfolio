-- Завершённые сделки из ods: единственное место, где dbt читает source().
select trade_id, trade_date, user_id, coin_id, side, amount_eur, fee_eur
from {{ source('ods', 'ods_trades') }}
where status = 'completed'
