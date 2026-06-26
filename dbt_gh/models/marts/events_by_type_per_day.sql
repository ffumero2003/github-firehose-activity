-- events_by_type_per_day.sql
-- Mart: total events per day, broken down by event type (all 4 types).
-- No dim join needed — date_key and event_type are both already on the fact.

select
    f.date_key,
    f.event_type,
    count(*) as event_count          -- one row per event → count per (day, type)
from {{ source('github_firehose', 'fact_events') }} f
group by 1, 2                         -- group by the two non-aggregate columns