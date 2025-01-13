with users as (
	select * from users_cumulated
	where date = date('2023-01-31')
),

series as (
	select * 
	from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
),

place_holder_ints as (
	select
		*,
		case when
			device_activity_datelist @> array[date(series_date)]
		then pow(2, 32 - (date - date(series_date)))
		else 0
		end as placeholder_int_value
	from users
	cross join series

)

select 
	user_id,
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as active_days_bits,
	bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 
		as dim_is_monthly_active,
	bit_count(cast('11111110000000000000000000000000' as bit(32)) & 
		cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0
		as dim_is_weekly_active,
	bit_count(cast('10000000000000000000000000000000' as bit(32)) & 
		cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0
		as dim_is_daily_active
from place_holder_ints
group by 1
