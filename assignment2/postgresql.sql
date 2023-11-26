 -- How many animals of each type have outcomes?
   SELECT animal_type, COUNT(DISTINCT animal_id) AS total_animals
   FROM dimension_animal
   GROUP BY animal_type;

 -- How many animals are there with more than 1 outcome?
 SELECT COUNT(animal_id) AS animals_with_multiple_outcomes
 FROM (
     SELECT animal_id
     FROM factanimal
     GROUP BY animal_id
     HAVING COUNT(DISTINCT outcome_type) > 1
 ) AS subquery;

 -- What are the top 5 months for outcomes?
 SELECT month, COUNT(*) AS total_outcomes
 FROM factanimal
 GROUP BY month
 ORDER BY total_outcomes DESC
 LIMIT 5;

 -- What is the total number percentage of kittens, adults, and seniors, whose outcome is "Adopted"?
 SELECT age, COUNT(animal_id) AS total, 
        (COUNT(animal_id) / (SELECT COUNT(*) FROM factanimal WHERE outcome_type = 'Adopted')) * 100 AS percentage
 FROM factanimal
 WHERE outcome_type = 'Adopted'
 GROUP BY age;

 -- Conversely, among all the cats who were "Adopted", what is the total number percentage of kittens, adults, and seniors?
 SELECT age, COUNT(animal_id) AS total, 
        (COUNT(animal_id) / (SELECT COUNT(*) FROM factanimal WHERE outcome_type = 'Adopted' AND animal_type = 'Cat')) * 100 AS percentage
 FROM factanimal
 WHERE outcome_type = 'Adopted' AND animal_type = 'Cat'
 GROUP BY age;

 -- For each date, what is the cumulative total of outcomes up to and including this date?
 SELECT ts, COUNT(*) AS cumulative_total
 FROM factanimal
 GROUP BY ts
 ORDER BY ts;
