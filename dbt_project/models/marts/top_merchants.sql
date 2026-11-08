{{ config(order_by='total_amount') }}

-- топ-10 мерчантов по обороту
select
    merchant,
    category,
    count() as tx_count,
    sum(amount) as total_amount
from {{ ref('stg_transactions') }}
group by merchant, category
order by total_amount desc
limit 10
