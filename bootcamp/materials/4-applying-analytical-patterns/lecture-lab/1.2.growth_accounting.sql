-- loop header
DO $$
DECLARE
    start_date date := date('2023-01-01');
    end_date date := date('2023-01-31'); -- Set your end date here
    cur_date date := start_date;
BEGIN
	WHILE cur_date <= end_date loop
-- loop header end

		INSERT INTO users_growth_accounting
        WITH yesterday AS (
        SELECT * FROM users_growth_accounting
        WHERE date = cur_date - interval '1 day'
    ),
        today AS (
            SELECT
                CAST(user_id AS TEXT) as user_id,
                DATE_TRUNC('day', event_time::timestamp) as today_date,
                COUNT(1)
            FROM events
            WHERE DATE_TRUNC('day', event_time::timestamp) = cur_date
            AND user_id IS NOT NULL
            GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
        )

            SELECT COALESCE(t.user_id, y.user_id)                    as user_id,
                    COALESCE(y.first_active_date, t.today_date)       AS first_active_date,
                    COALESCE(t.today_date, y.last_active_date)        AS last_active_date,
                    CASE
                        WHEN y.user_id IS NULL THEN 'New'
                        WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
                        WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
                        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
                        ELSE 'Stale'
                        END                                           as daily_active_state,
                    CASE
                        WHEN y.user_id IS NULL THEN 'New'
                        WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
                        WHEN
                                t.today_date IS NULL
                                AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
                        WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date THEN 'Retained'
                        ELSE 'Stale'
                        END                                           as weekly_active_state,
                    COALESCE(y.dates_active,
                            ARRAY []::DATE[])
                        || CASE
                            WHEN
                                t.user_id IS NOT NULL
                                THEN ARRAY [t.today_date]
                            ELSE ARRAY []::DATE[]
                        END                                           AS date_list,
                    COALESCE(t.today_date, y.date + Interval '1 day') as date
            FROM today t
                    FULL OUTER JOIN yesterday y
                                    ON t.user_id = y.user_id;

-- loop footer, Increment the current date
        cur_date := cur_date + interval '1 day';
    END LOOP;
END $$;
-- loop footer end