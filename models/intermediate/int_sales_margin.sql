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
    , round(s.revenue,2) as revenue
    , s.quantity
    , p.purchase_price
    , round(revenue * quantity,2) as total_revenue
    , round(purchase_price * quantity,2) as purchase_cost
    , (revenue-purchase_price)*quantity as margin
from sales as s left join {{ref("stg_raw__product")}} as p
using (product_id)