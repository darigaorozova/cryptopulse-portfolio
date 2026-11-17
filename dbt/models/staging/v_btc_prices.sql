select price_date, price_eur
from {{ source('ods', 'ods_coin_prices') }}
where coin_id = 'bitcoin'
