
  
    

    create or replace table `github-firehose-fumero`.`github_firehose`.`pr_throughput`
      
    
    

    
    OPTIONS()
    as (
      -- pr_throughput.sql
-- Mart: pull request activity per day, broken down by action (opened/closed/merged).
-- Filter to PullRequestEvent only, then count per day + action.

select
    f.date_key,
    f.action,                        -- opened / closed / reopened / etc.
    count(*) as pr_count
from `github-firehose-fumero`.`github_firehose`.`fact_events` f
where f.event_type = 'PullRequestEvent'   -- filter early to PRs only
group by 1, 2
    );
  