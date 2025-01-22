-- change tracking

WITH 

yesterday AS (
        SELECT * FROM player_seasons_growth_accounting
        WHERE season = 1995
    ),
        today AS (
            SELECT
                player_name,
                season
            FROM player_seasons
            WHERE season = 1996
            AND player_name IS NOT NULL
        )

            SELECT COALESCE(t.player_name, y.player_name) as player_name,
                    COALESCE(y.draft_year, t.season) AS draft_year,
                    COALESCE(t.season, y.last_active_season) AS last_active_season,
                    CASE
                        WHEN y.player_name IS NULL THEN 'New'
                        WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
                        WHEN y.last_active_season < t.season - 1 THEN 'Returned from Retirement'
                        WHEN t.season IS NULL AND y.last_active_season = y.season THEN 'Retired'
                        ELSE 'Stayed Retired'
                        END as player_state,
                    COALESCE(y.season_list,
                            ARRAY [])
                        || CASE
                            WHEN
                                t.player_name IS NOT NULL
                                THEN ARRAY [t.season]
                            ELSE ARRAY []
                        END                                           AS season_list,
                    COALESCE(t.season, y.season + 1) as season
            FROM today t
                    FULL OUTER JOIN yesterday y
                                    ON t.player_name = y.player_name;