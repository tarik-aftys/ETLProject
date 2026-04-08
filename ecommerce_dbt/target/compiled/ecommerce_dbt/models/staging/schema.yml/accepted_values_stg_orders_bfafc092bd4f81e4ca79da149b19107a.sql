
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "ecommerce_dw"."core"."stg_orders"
    group by status

)

select *
from all_values
where value_field not in (
    'created','approved','invoiced','processing','shipped','delivered','unavailable','canceled'
)


