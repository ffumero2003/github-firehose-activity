-- issue_activity.sql
-- Mart: issue activity per day, broken down by action (opened/closed/reopened).
select
    f.date_key,
    f.action,
    count(*) as issue_count
from `github-firehose-fumero`.`github_firehose`.`fact_events` f
where f.event_type = 'IssuesEvent'
group by 1, 2