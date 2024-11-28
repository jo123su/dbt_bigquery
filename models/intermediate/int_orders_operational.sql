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
)

select
    orders_id
    , date_date
    , ROUND(SUM(revenue),2) as revenue
    , SUM(quantity) as quantity
    , ROUND(SUM(purchase_cost),2) as purchase_cost
    , ROUND(SUM(revenue)- SUM(purchase_cost),2) as margin
FROM
    orders_added_cost
GROUP BY
    orders_id, date_date