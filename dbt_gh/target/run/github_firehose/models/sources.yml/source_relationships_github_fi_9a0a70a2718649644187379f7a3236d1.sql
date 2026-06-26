
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select actor_id as from_field
    from `github-firehose-fumero`.`github_firehose`.`fact_events`
    where actor_id is not null
),

parent as (
    select actor_id as to_field
    from `github-firehose-fumero`.`github_firehose`.`dim_actor`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test