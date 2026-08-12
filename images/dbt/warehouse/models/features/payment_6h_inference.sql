with source_data as (
    select * from {{ ref('payment_6h') }}
),

row_number_table as (
    select
        *,
        row_number() over (partition by account_id, card_id order by window_start) as _row_number
    from polaris.features.payment_6h
)

select
    window_start
    ,window_end
    ,account_id
    ,card_id
    ,tx_count
    ,total_amount
from row_number_table
where _row_number = 1
