CREATE TABLE IF NOT EXISTS songplay_fact (
	start_time TIMESTAMP REFERENCES time_dim (start_time),
	user_pk SERIAL REFERENCES user_dim (user_pk),
	level VARCHAR NOT NULL,
	song_pk SERIAL REFERENCES song_dim (song_pk),
	artist_pk SERIAL REFERENCES artist_dim (artist_pk),
	session_id INT NOT NULL,
	location VARCHAR,
	user_agent TEXT,
	PRIMARY KEY (start_time, user_pk, song_pk, artist_pk)
);