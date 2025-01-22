-- grouping sets aggregation
WITH games_augmented AS (
    SELECT COALESCE(d.player_name, 'unknown')      AS player_name,
           COALESCE(d.team_abbreviation, 'unknown')  AS team,
           COALESCE(g.season, 'unknown') AS season
    FROM games g
             JOIN game_details d on g.game_id = d.game_id
)

SELECT
       CASE
           WHEN GROUPING(player_name) = 0
               AND GROUPING(team) = 0
               THEN 'player_name__team'
           WHEN GROUPING(player_name) = 0
                AND GROUPING(season) = 0 THEN 'player_name__season'
           WHEN GROUPING(team) = 0 THEN 'team'
       END as aggregation_level,
       COALESCE(player_name, '(overall)') as player_name,
       COALESCE(team, '(overall)') as team,
       COALESCE(season, '(overall)') as season,
       COUNT(1) as number_of_hits
FROM games_augmented
GROUP BY GROUPING SETS (
        (player_name, team),
        (player_name, season),
        (team)
    )
ORDER BY COUNT(1) DESC