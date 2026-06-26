-- top_contributors.sql
-- Mart: which actors created the most events.
-- Fact (events) joined to dim_actor for the readable login.

select
    f.actor_id,
    a.actor_login,
    count(*) as event_count          -- one row per event → count per actor
from {{ source('github_firehose', 'fact_events') }} f
left join {{ source('github_firehose', 'dim_actor') }} a
    on f.actor_id = a.actor_id
group by 1, 2                         -- group by the non-aggregate columns (actor_id, actor_login)