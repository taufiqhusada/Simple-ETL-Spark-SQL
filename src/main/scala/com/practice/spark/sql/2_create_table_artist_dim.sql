CREATE TABLE  IF NOT EXISTS artist_dim (
	artist_pk SERIAL PRIMARY KEY,
	artist_id VARCHAR,
	name VARCHAR,
	location VARCHAR,
	latitude DECIMAL(9,6),
	longitude DECIMAL(9,6)
);