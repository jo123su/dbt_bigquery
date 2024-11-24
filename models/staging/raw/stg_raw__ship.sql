with
    source as (
        select * 
        from {{ source("raw", "ship") }}
    ),

    renamed as (
        select 
            orders_id,
            CAST(ship_cost AS INT) AS ship_cost, -- Casteo corregido
            shipping_fee,
            logCost
        from source
    )

select *
from renamed