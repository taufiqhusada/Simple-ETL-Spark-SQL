CREATE TABLE IF NOT EXISTS user_dim (
    user_pk SERIAL PRIMARY KEY,
    user_id INT,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR NOT NULL
);