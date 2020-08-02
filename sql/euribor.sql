-- "indexes".euribor definition

-- Drop table

-- DROP TABLE "indexes".euribor;

CREATE TABLE "indexes".euribor (
	eur_date date NULL,
	eur_1w numeric NULL,
	eur_1m numeric NULL,
	eur_3m numeric NULL,
	eur_6m numeric NULL,
	eur_12m numeric NULL,
	date_insertion timestamp NULL
);
CREATE INDEX euribor_date_idx ON indexes.euribor USING btree (eur_date);