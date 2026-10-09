CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE spark_streams;

CREATE TABLE IF NOT EXISTS created_users (
    id text PRIMARY KEY,
    first_name text,
    last_name text,
    gender text,
    address text,
    post_code text,
    email text,
    username text,
    dob text,
    registered_date text,
    phone text,
    picture text
);