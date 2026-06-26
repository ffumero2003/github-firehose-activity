-- events_per_repo_per_day.sql
-- Mart: how many events each repo had, per day, by type.
-- Joins the fact to dim_repo for the readable repo name.

select
    f.date_key,
    f.repo_id,
    r.repo_name,
    r.repo_owner,
    f.event_type,
    count(*) as event_count          -- one row per event → count them
from `github-firehose-fumero`.`github_firehose`.`fact_events` f
left join `github-firehose-fumero`.`github_firehose`.`dim_repo` r
    on f.repo_id = r.repo_id
group by 1, 2, 3, 4, 5               -- group by everything that isn't the aggregate