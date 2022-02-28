with 

customers as (
    select * from {{ ref('stg_customers') }}
),

fct_orders as (
    select * from {{ ref('fct_orders') }}
),

customer_orders as (

    select
        fo.customer_id,
        min(fo.order_date) as first_order_date,
        max(fo.order_date) as most_recent_order_date,
        count(fo.order_id) as number_of_orders,
        sum(fo.amount) as lifetime_value
    from
        fct_orders as fo
    group by 
      fo.customer_id

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.most_recent_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders,
        coalesce(co.lifetime_value, 0) as lifetime_value
    from 
        customers as c left join customer_orders as co using(customer_id)
    
)

select * from final