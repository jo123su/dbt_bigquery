with orders_added_cost AS 
(
select
    s.orders_id
    , s.date_date
    , s.revenue
    , s.quantity
    , p.purchase_price
    , p.purchase_price * s.quantity as purchase_cost 
from {{ref("stg_raw__sales")}} as s left join {{ref("stg_raw__product")}} as p 
using (product_id)
),

orders_operational as (select
    orders_id
    , date_date
    , ROUND(SUM(revenue),2) as revenue
    , SUM(quantity) as quantity
    , ROUND(SUM(purchase_cost),2) as purchase_cost
    , ROUND(SUM(revenue)- SUM(purchase_cost),2) as margin
    
FROM orders_added_cost 
GROUP BY
    orders_id, date_date)


Select 
    op.*
    , sp.logCost
    , sp.ship_cost
    , sp.shipping_fee
    , margin + shipping_fee - logCost - ship_cost AS operational_margin
FROM orders_operational  as op left join {{ref("stg_raw__ship")}} as sp
using (orders_id)