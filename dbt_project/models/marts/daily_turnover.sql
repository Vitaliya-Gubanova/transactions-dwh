{{ config(order_by='event_date') }}

-- оборот по дням
select
    event_date,
    count() as tx_count,
    uniq(client_id) as clients,
    sum(amount) as total_amount,
    round(avg(amount), 2) as avg_amount
from {{ ref('stg_transactions') }}
group by event_date
