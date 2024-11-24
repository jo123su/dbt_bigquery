with sales as (
    select
    product_id
    , sum(revenue) as revenue
    , sum(quantity) as quantity
from {{ref("stg_raw__sales")}}
GROUP BY product_id
)

select
    s.product_id
    , s.revenue
    , s.quantity
    , p.purchase_price
from sales as s left join {{ref("stg_raw__product")}} as p
using (product_id)