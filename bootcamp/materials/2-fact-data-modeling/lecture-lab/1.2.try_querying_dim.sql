SELECT 
	dim_player_name,
	count(1) as num_games,
	count(case when dim_not_with_team then 1 end) as bail,
	cast(count(case when dim_not_with_team then 1 end) as real)/count(1) as bail_pct
FROM fct_game_details
group by 1
order by 4 desc;