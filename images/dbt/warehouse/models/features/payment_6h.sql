with source_data as (
    select * from {{ source('raw', 'payment_authorised') }}
)

,bounds as (
    SELECT
        CAST(date_trunc('day', MIN(timestamp)) - INTERVAL '1' DAY AS TIMESTAMP) AS start_bound,
        CAST(date_trunc('day', MAX(timestamp)) + INTERVAL '1' DAY AS TIMESTAMP) AS end_bound
    FROM source_data
)
,timeline as (
    SELECT window_end
    FROM bounds b
    CROSS JOIN UNNEST(
        SEQUENCE(
            b.start_bound
            ,b.end_bound
            ,INTERVAL '5' MINUTE
        )
    ) AS t(window_end)
)
SELECT
    t.window_end
    ,t.window_end - INTERVAL '6' HOUR AS window_start
    ,account_id
    ,card_id
    ,count(*) AS tx_count
    ,sum(amount) AS total_amount
FROM source_data p
INNER JOIN timeline t
    ON p.timestamp > t.window_end - INTERVAL '6' HOUR
    AND p.timestamp <= t.window_end
GROUP BY window_end, account_id, card_id
