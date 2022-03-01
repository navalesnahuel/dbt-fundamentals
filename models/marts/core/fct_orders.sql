with 

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments as (
    select
        p.order_id,
        sum(case when p.status = 'success' then p.amount else 0 end) as amount
    from 
        payments as p
    group by p.order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        coalesce(op.amount, 0) as amount
    from
        orders as o left join order_payments as op using(order_id)
)

select * from final