create type films as (
    film text,
    votes integer,
    rating float,
    filmid integer
);

create type quality_class as enum (
    'star',
    'good',
    'average',
    'bad'
);

create table actors (
    films films[],
    quality_class quality_class,
    is_active boolean
);
