-- "indexes".euribor definition

-- Drop table

-- DROP TABLE "indexes".euribor;

CREATE TABLE "orchard".euribor (
	eur_date date PRIMARY KEY,
	eur_1w numeric NULL,
	eur_1m numeric NULL,
	eur_3m numeric NULL,
	eur_6m numeric NULL,
	eur_12m numeric NULL,
	eur_year numeric NULL,
	eur_month numeric NULL,
	date_insertion timestamp NULL
);
CREATE INDEX euribor_date_idx ON "orchard".euribor USING btree (eur_date);