
insert into users_cumulated
with yesterday as (
	select *
	from users_cumulated
	where date = date('2022-12-31')
),

today as (
	select
		cast (user_id as text) as user_id,
        browser_type,
		date(cast(event_time as timestamp)) as date_active
	from events
	where date(cast(event_time as timestamp)) = date('2023-01-01')
		and user_id is not null
	group by 1, 2, 3
	
)

select
	coalesce(t.user_id, y.user_id) as user_id,
    coalesce(t.browser_type, y.browser_type) as browser_type,
	case 
		when y.device_activity_datelist is null then array[t.date_active]
		when t.date_active is null then y.device_activity_datelist
		else array[t.date_active] || y.device_activity_datelist
		end as device_activity_datelist,
	coalesce(t.date_active, y.date + interval '1 day') as date
from today t
full outer join yesterday y
	on t.user_id = y.user_id;


select * from users_cumulated;