create table array_metrics ( 
	user_id numeric,
	month_start date,
	metric_name text,
	metric_array real[],
	primary key (user_id, month_start, metric_name)
);

insert into array_metrics
	with daily_aggregate as ( 
		select
			user_id,
			date(event_time) as date,
			count(1) as num_site_hits
		from events
		where date(event_time) = date('2023-01-03')
		group by 1, 2
	),
	
	yesterday_array as (
		select * from array_metrics
		where month_start = date('2023-01-01')
	)
	
	select
		coalesce(da.user_id, ya.user_id) as user_id,
		coalesce(ya.month_start, date_trunc('month', da.date)) as month_start,
		'site_hits' as metric_name,
		case
			when ya.metric_array is not null 
				then ya.metric_array || array[coalesce(da.num_site_hits, 0)]
			when ya.metric_array is null
				then array_fill(0, ARRAY[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.num_site_hits, 0)]
		end as metric_array
		
		
	from daily_aggregate da
	full outer join yesterday_array ya
		on da.user_id = ya.user_id
	WHERE COALESCE(da.user_id, ya.user_id) IS NOT NULL
	on conflict (user_id, month_start, metric_name)
	do
		update set metric_array = EXCLUDED.metric_array;