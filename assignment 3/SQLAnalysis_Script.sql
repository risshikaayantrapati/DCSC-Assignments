--How many animals of each type have outcomes?
select a.animal_type, count(distinct a.animal_id) from animaldimension a,
outcomesfact o, outcomedimension od where o.animal_dim_key = a.animal_dim_key 
and o.outcome_dim_key = od.outcome_dim_key and od.outcome_type is not null
group by a.animal_type ;

--How many animals are there with more than 1 outcome?
select count(animal_id) as animals_with_morethan_one_count 
from(select a.animal_id from animaldimension a,
outcomesfact o, outcomedimension od where o.animal_dim_key = a.animal_dim_key 
and o.outcome_dim_key = od.outcome_dim_key and od.outcome_type is not null
group by a.animal_id having count(*)>1)as query;

--What are the top 5 months for outcomes? 
select t.monthh,count(t.monthh) as counter from timingdimension t , outcomedimension o , outcomesfact o2 
where o2.outcome_dim_key  = o.outcome_dim_key and o2.time_dim_key = t.time_dim_key 
and o.outcome_type is not null
group by t.monthh
order by counter desc 
limit 5;

--What is the total number percentage of kittens, adults, and seniors, whose outcome is "Adopted"?

select sum(counter) as TotalAdoptedCats from
(select
	cat_age_grp,
	COUNT(*) as counter
from
	(
	select
		ad.animal_dim_key,
		case
			when AGE(ad.dob) < interval '1 year' then 'Kitten'
			when AGE(ad.dob) >= interval '1 year'
			and AGE(ad.dob) <= interval '10 years' then 'Adult'
			when AGE(ad.dob) > interval '10 years' then 'Senior'
			else 'Unknown'
		end as cat_age_grp
	from
		animaldimension ad where ad.animal_type = 'Cat'
) as cat_agegroup
join outcomesfact of2 on
	cat_agegroup.animal_dim_key = of2.animal_dim_key 
join outcomedimension otd on
	of2.outcome_dim_key  = otd.outcome_dim_key 
where
	otd.outcome_type  = 'Adoption'
group by
	cat_age_grp) as query;

--Conversely, among all the cats who were "Adopted", what is the total number percentage of kittens, adults, and seniors?

WITH Cat_Ages AS (
  SELECT
    ad.animal_dim_key,
    CASE
      WHEN AGE(ad.dob) < INTERVAL '1 year' THEN 'Kittens'
      WHEN AGE(ad.dob) >= INTERVAL '1 year' AND AGE(ad.dob) <= INTERVAL '10 years' THEN 'Adults'
      WHEN AGE(ad.dob) > INTERVAL '10 years' THEN 'Seniors'
      ELSE 'Unknown'
    END AS cat_age_grp
  FROM animaldimension ad where ad.animal_type = 'Cat'
)

SELECT
  cat_age_grp,
  COUNT(*) AS toalcount
FROM outcomesfact o
JOIN Cat_Ages cat ON o.animal_dim_key  = cat.animal_dim_key
JOIN outcomedimension od ON o.outcome_dim_key  = od.outcome_dim_key 
WHERE od.outcome_type  = 'Adoption'
GROUP BY cat_age_grp;

--For each date, what is the cumulative total of outcomes up to and including this date?
SELECT 
    date(a.timestmp) as date_only,
    count(a.animal_dim_key) as outcomes,
    SUM(count(a.animal_dim_key)) OVER (ORDER BY date(a.timestmp)) AS cumulative_total
FROM 
    animaldimension a left join outcomesfact o2 on a.animal_dim_key =o2.animal_dim_key
    left join outcomedimension o on o2.outcome_dim_key = o.outcome_dim_key 
where o.outcome_type is not null
group by date(a.timestmp)
ORDER BY 
    date_only;


