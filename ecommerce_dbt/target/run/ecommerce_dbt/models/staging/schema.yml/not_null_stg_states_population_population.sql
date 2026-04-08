select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select population
from "ecommerce_dw"."core"."stg_states_population"
where population is null



      
    ) dbt_internal_test