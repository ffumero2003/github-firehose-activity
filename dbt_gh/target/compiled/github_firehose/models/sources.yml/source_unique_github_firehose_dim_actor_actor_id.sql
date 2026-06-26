
    
    

with dbt_test__target as (

  select actor_id as unique_field
  from `github-firehose-fumero`.`github_firehose`.`dim_actor`
  where actor_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


