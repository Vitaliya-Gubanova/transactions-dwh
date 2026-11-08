-- чистим сырые данные:
--  * выкидываем транзакции с суммой <= 0 (приходят от генератора как брак)
--  * убираем дубли по transaction_id (при перезапуске consumer возможна повторная загрузка)

select
    transaction_id,
    client_id,
    merchant,
    category,
    amount,
    currency,
    event_time,
    toDate(event_time) as event_date
from {{ source('raw', 'transactions') }}
where amount > 0
order by transaction_id, loaded_at
limit 1 by transaction_id
