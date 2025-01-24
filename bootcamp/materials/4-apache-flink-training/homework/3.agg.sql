with sessionized_events as (
    select
        host,
        session_start,
        session_end,
        count(1) as num_events
    from processed_events_aggregated
    group by host, session_start, session_end
)

select
    host,
    avg(num_events) as avg_events_per_session
from sessionized_events
where host in ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by host