
CREATE TABLE IF NOT EXISTS dimension_animal (
animal_id VARCHAR PRIMARY KEY,
name VARCHAR,
date_of_birth date,
animal_type VARCHAR,
breed VARCHAR,
color VARCHAR
);

CREATE TABLE IF NOT EXISTS dimension_time (
date_id serial PRIMARY KEY,
datetime TIMESTAMP,
month VARCHAR,
year INT
);

CREATE TABLE IF NOT EXISTS dimension_outcometype (
outcome_id serial PRIMARY KEY,
outcome_type VARCHAR,
outcome_subtype VARCHAR
);

CREATE TABLE IF NOT EXISTS factanimal (
breed_id serial PRIMARY KEY,
animal_id VARCHAR,
date_id INT,
outcome_id INT,
month VARCHAR,
year INT,
outcome_type VARCHAR,
FOREIGN KEY (animal_id) REFERENCES dimension_animal(animal_id),
FOREIGN KEY (date_id) REFERENCES dimension_time(date_id),
FOREIGN KEY (outcome_id) REFERENCES dimension_outcometype(outcome_id)
);
