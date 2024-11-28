select 
    date_date
    , count(*) as nb_transactions
    , ROUND(sum(revenue),2) as revenue
    , sum(revenue)/count(*) as average_basket
    , SUM(operational_margin) as operational_margin
    , ROUND(sum(purchase_cost),2) as purchase_cost
    , SUM(shipping_fee) as shipping_fee
    , ROUND(sum(margin),2) as margin
    , ROUND(sum(logCost),2) as logCost
    , SUM(quantity) as quantity
from {{ref("int_orders_margin")}}
where date_date = '2021-09-30'
GROUP BY date_date
