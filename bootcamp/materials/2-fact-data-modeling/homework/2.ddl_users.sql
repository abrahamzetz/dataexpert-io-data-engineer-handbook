create table users_cumulated (
	user_id text,
    browser_type text,
	device_activity_datelist date[], -- the list of dates in the past where the user was active
	date date, -- the current date for the user
	primary key (user_id, browser_type, date)
);