WITH 

current_year as (
	select 1970 as current_year
),

last_year as (
    select * from actors, current_year cy
    where year = cy.current_year - 1
),

this_year as (
    select * from actor_films, current_year cy
    where year = cy.current_year
)

INSERT INTO actors
select distinct
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actorid, ty.actorid) as actorid,
    cy.current_year as year,
    case when ty.year is not null then
        array_agg(row(ty.film, ty.votes, ty.rating, ty.filmid)::films)
    else ly.films
    end as films,
    case when ty.year is not null then
        case when avg(ty.rating) > 8 then 'star'::quality_class
             when avg(ty.rating) > 7 then 'good'::quality_class
             when avg(ty.rating) > 6 then 'average'::quality_class
             else 'bad'::quality_class
        end
    else ly.quality_class
    end as quality_class,
    ty.year is not null as is_active
from last_year ly
full outer join this_year ty
    on ly.actorid = ty.actorid,
current_year cy
group by 1, 2, 3, 6, ly.films, ly.quality_class;