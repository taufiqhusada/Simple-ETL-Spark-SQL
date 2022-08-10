CREATE TABLE IF NOT EXISTS song_dim (
    song_pk SERIAL PRIMARY KEY,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    artist_pk  SERIAL REFERENCES artist_dim (artist_pk),
    year INT CHECK (year >= 0),
    duration FLOAT
);