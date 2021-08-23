--SQL to display total covid cases and deaths in US based on sample data provided
select sum(total_cases) as us_total_cases
	, sum(total_deaths) as us_total_deaths
from (
		select state, total_cases, total_deaths,
		row_number() over (partition by state order by submission_date desc) as row_number
		from us_covid_sample
	) t
where row_number = 1;

--SQL to display top 5 states with high covid cases based on sample data provided
select state
	, total_cases
from (
		select state
		, total_cases
		, row_number() over (partition by state order by submission_date desc) as row_number
		from us_covid_sample
	) t
where row_number = 1
order by total_cases desc
limit 5;

--SQL to display Total cases and death in California as of 5/20/2021 
--based on sample data provided
select total_cases
	, total_deaths
from us_covid_sample
where state = 'CA'
and submission_date = '2021-05-20';

--SQL to display when percent increase of Covid cases is more than 20% 
--compared to previous submission in California based on sample data provided
select submission_date
, COALESCE(ROUND((total_cases - prev_day_case) / prev_day_case * 100), 0) AS percent_change
 from (
	select submission_date, state, new_case, total_cases,
	lag(total_cases) OVER (partition by state order by submission_date asc) as prev_day_case
	from us_covid_sample
	where state = 'CA'  and total_cases > 0
 ) t
 where COALESCE(ROUND((total_cases - prev_day_case) / prev_day_case * 100), 0) > 20;


--SQL to display safest state comparatively based on sample data provided
select state
, total_cases
from (
	select state
		, total_cases
		, dense_rank() over (order by total_cases asc) cases_rank
	from (
			select state
			, total_cases
			, total_deaths
			, row_number() over (partition by state order by submission_date desc) as row_number
			from us_covid_sample
		) t
	where row_number = 1
) p
where cases_rank <=1;
