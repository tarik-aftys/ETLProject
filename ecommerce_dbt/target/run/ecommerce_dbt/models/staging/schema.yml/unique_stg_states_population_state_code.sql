select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    state_code as unique_field,
    count(*) as n_records

from "ecommerce_dw"."core"."stg_states_population"
where state_code is not null
group by state_code
having count(*) > 1



      
    ) dbt_internal_test