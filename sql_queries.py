import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR(150),
        auth VARCHAR(15),
        first_name VARCHAR(100),
        gender VARCHAR(1),
        item_in_session INT NOT NULL,
        last_name VARCHAR(100),
        length FLOAT,
        level VARCHAR(10),
        location VARCHAR(100),
        method VARCHAR(5),
        page VARCHAR(50),
        registration BIGINT,
        session_id INT,
        song VARCHAR(250),
        status INT,
        ts BIGINT,
        user_agent TEXT,
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT,
        artist_id TEXT NOT NULL,
        artist_latitude TEXT,
        artist_longitude TEXT,
        artist_location TEXT,
        artist_name TEXT NOT NULL,
        song_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        duration FLOAT,
        year INT
    );
""")
    

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(0,1),
        start_time BIGINT NOT NULL,
        user_id INT NOT NULL,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id INT NOT NULL,
        location TEXT,
        user_agent TEXT
    );
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender VARCHAR(1),
        level TEXT
    );
""")


song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id TEXT,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year INT,
        duration NUMERIC
    );
""")


artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id TEXT,
        name TEXT NOT NULL,
        location TEXT,
        latitude TEXT,
        longitude TEXT
    );
""")


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    COMPUPDATE OFF
    JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["arn"], config["S3"]["LOG_JSONPATH"])


staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    COMPUPDATE OFF
    FORMAT AS json 'auto';
""").format(config["S3"]["song_data"], config["IAM_ROLE"]["arn"])


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT se.ts AS start_time,
            se.user_id AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.session_id AS session_id,
            se.location AS location,
            se.user_agent AS user_agent
    FROM staging_songs AS ss
    INNER JOIN staging_events AS se ON ss.title = se.song
    INNER JOIN staging_events AS se2 ON se2.artist = ss.artist_name;
""")


user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT se.user_id,
           se.first_name,
           se.last_name,
           se.gender,
           se.level
    FROM staging_events AS se
    WHERE se.first_name IS NOT NULL;
""")


song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")


artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
    FROM staging_songs;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT (timestamp 'epoch' + ts * interval '1 second'/1000) AS start_time,
            date_part(hour, start_time) AS hour,
            date_part(day, start_time) AS day,
            date_part(week, start_time) AS week,
            date_part(month, start_time) AS month,
            date_part(year, start_time) AS year,
            date_part(weekday, start_time) AS weekday
    FROM staging_events
""")


# TODO: Removing duplicates in 'users' and 'artists' tables.
temp_user_table_create = ("""
    CREATE TABLE IF NOT EXISTS temp_users(
        user_id INT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender VARCHAR(1),
        level TEXT
    );
""")


temp_artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS temp_artists(
        artist_id TEXT,
        name TEXT NOT NULL,
        location TEXT,
        latitude TEXT,
        longitude TEXT
    );
""")


load_temp_users = ("""
    INSERT INTO temp_users
        SELECT DISTINCT(user_id), first_name, last_name, gender, level FROM users;
""")


load_temp_artists = ("""
    INSERT INTO temp_artists
        SELECT DISTINCT(artist_id), name, location, latitude, longitude FROM artists;
""")


rename_temp_user = ("""
    ALTER TABLE temp_users RENAME TO users
""")


rename_temp_artist = ("""
    ALTER TABLE temp_artists RENAME TO artists
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
load_staging_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
remove_duplicate_users_queries = [temp_user_table_create, load_temp_users, user_table_drop, rename_temp_user]
remove_duplicate_artists_queries = [temp_artist_table_create, load_temp_artists, artist_table_drop, rename_temp_artist]