create table users_cumulated (
	user_id text,
	dates_active date[], -- the list of dates in the past where the user was active
	date date, -- the current date for the user
	primary key (user_id, date)
);

insert into users_cumulated
with yesterday as (
	select *
	from users_cumulated
	where date = date('2022-12-31')
),

today as (
	select
		cast (user_id as text) as user_id,
		date(cast(event_time as timestamp)) as date_active
	from events
	where date(cast(event_time as timestamp)) = date('2023-01-01')
		and user_id is not null
	group by 1, 2
	
)

select
	coalesce(t.user_id, y.user_id) as user_id,
	case 
		when y.dates_active is null then array[t.date_active]
		when t.date_active is null then y.dates_active
		else array[t.date_active] || y.dates_active
		end as dates_active,
	coalesce(t.date_active, y.date + interval '1 day') as date
from today t
full outer join yesterday y
	on t.user_id = y.user_id;


select * from users_cumulated;