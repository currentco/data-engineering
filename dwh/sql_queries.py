import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")

LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
"artist" varchar,
"auth" varchar,
"firstName" varchar,
"gender" varchar,
"itemInSession" int,
"lastName" varchar,
"length" numeric,
"level" varchar,
"location" varchar,
"method" varchar,
"page" varchar,
"registration" numeric,
"sessionId" int,
"song" varchar,
"status" int,
"ts" timestamp,
"userAgent" varchar,
"userId" int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
"song_id" varchar NOT NULL,
"num_songs" int,
"title" varchar,
"artist_name" varchar,
"artist_latitude" numeric,
"year" int,
"duration" numeric,
"artist_id" varchar,
"artist_longitude" numeric,
"artist_location" varchar
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
"songplay_id" int IDENTITY(0,1) NOT NULL SORTKEY PRIMARY KEY, 
"start_time" timestamp NOT NULL, 
"user_id" int NOT NULL, 
"level" varchar, 
"song_id" varchar NOT NULL DISTKEY, 
"artist_id" varchar NOT NULL, 
"session_id" int NOT NULL, 
"location" varchar, 
"user_agent" varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
"user_id" int NOT NULL SORTKEY PRIMARY KEY, 
"first_name" varchar, 
"last_name" varchar, 
"gender" varchar, 
"level" varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
"song_id" varchar NOT NULL SORTKEY DISTKEY PRIMARY KEY, 
"title" varchar, 
"artist_id" varchar NOT NULL, 
"year" int, 
"duration" numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
"artist_id" varchar NOT NULL SORTKEY PRIMARY KEY, 
"name" varchar, 
"location" varchar, 
"latitude" numeric, 
"longitude" numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
"start_time" timestamp NOT NULL SORTKEY PRIMARY KEY, 
"hour" int, 
"day" int, 
"week" int, 
"month" int, 
"year" int, 
"weekday" int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
                        iam_role {}
                        json {}
                        timeformat as 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
                       iam_role {}
                       json 'auto ignorecase'
                       timeformat as 'epochmillisecs';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(
    start_time, user_id, level, song_id, 
    artist_id, session_id, location, user_agent
) 
SELECT DISTINCT staging_events.ts AS start_time, 
                staging_events.userId AS user_id, 
                staging_events.level, 
                staging_songs.song_id, 
                staging_songs.artist_id, 
                staging_events.sessionId AS session_id, 
                staging_events.location, 
                staging_events.userAgent AS user_agent
FROM staging_events
JOIN staging_songs ON staging_events.song = staging_songs.title
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
(
    user_id, first_name, last_name, gender, level
) 
SELECT DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
FROM staging_events
WHERE page = 'NextSong' AND
user_id NOT IN (SELECT DISTINCT user_id FROM users) AND
user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs 
(
    song_id, title, artist_id, year, duration
) 
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists 
(
    artist_id, name, location, latitude, longitude
) 
SELECT DISTINCT artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time 
(
    start_time, hour, day, week, month, year, weekday
) 
SELECT DISTINCT(start_time) AS start_time,
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month,
EXTRACT(year FROM start_time) AS year,
EXTRACT(dow FROM start_time) as weekday
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
