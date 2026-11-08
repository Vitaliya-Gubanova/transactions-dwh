{{ config(order_by='(event_date, category)') }}

-- траты по категориям за каждый день
select
    event_date,
    category,
    count() as tx_count,
    sum(amount) as total_amount,
    round(avg(amount), 2) as avg_amount
from {{ ref('stg_transactions') }}
group by event_date, category
