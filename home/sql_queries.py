# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id SERIAL PRIMARY KEY,
    start_time BIGINT NOT NULL,
    user_id TEXT NOT NULL,
    level TEXT,
    artist_id TEXT,
    song_id TEXT,
    session_id TEXT,
    location TEXT,
    user_agent TEXT,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (start_time) REFERENCES time (start_time));
""")

user_table_create = ("""
CREATE TABLE users(
    user_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT,
    year INT,
    duration DECIMAL,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id))
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude DECIMAL,
    longitude DECIMAL)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time BIGINT PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, artist_id, song_id, session_id, location, user_agent)
VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
# Find the song ID and artist ID based on the title, artist name, and duration of a song.
# Seems really inefficient to execute a query per lookup.
# TODO: find a more efficient of processing the data

song_select = ("""
SELECT
    s.song_id,
    s.artist_id
FROM
    songs s inner join artists a on s.artist_id = a.artist_id
WHERE
    s.title = %s
    and s.duration = %s
    and a.name = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, song_table_drop, user_table_drop, artist_table_drop, time_table_drop]
