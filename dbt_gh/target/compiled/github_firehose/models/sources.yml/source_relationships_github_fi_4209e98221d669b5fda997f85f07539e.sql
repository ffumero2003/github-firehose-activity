
    
    

with child as (
    select repo_id as from_field
    from `github-firehose-fumero`.`github_firehose`.`fact_events`
    where repo_id is not null
),

parent as (
    select repo_id as to_field
    from `github-firehose-fumero`.`github_firehose`.`dim_repo`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


