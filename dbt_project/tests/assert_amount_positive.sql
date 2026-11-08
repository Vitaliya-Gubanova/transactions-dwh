-- в чистом слое не должно остаться транзакций с неположительной суммой
select *
from {{ ref('stg_transactions') }}
where amount <= 0
