CREATE TABLE IF NOT EXISTS track (
    id SERIAL PRIMARY KEY,
    track_id VARCHAR(100) NOT NULL UNIQUE,
    track_name VARCHAR(100) NOT NULL,
    album_name VARCHAR(100) NOT NULL,
    artist_name VARCHAR(100) NOT NULL,
    track_popularity INT NOT NULL,
    track_uri VARCHAR(100) NOT NULL
);