import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events;'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs;'
songplay_table_drop = 'DROP TABLE IF EXISTS songplays;'
user_table_drop = 'DROP TABLE IF EXISTS users;'
song_table_drop = 'DROP TABLE IF EXISTS songs;'
artist_table_drop = 'DROP TABLE IF EXISTS artists;'
time_table_drop = 'DROP TABLE IF EXISTS time;'

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR,
auth VARCHAR,
firstname VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INT,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId INT)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INT,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INT)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
start_time BIGINT NOT NULL,
user_id varchar NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration DECIMAL(10,5))
SORTKEY (song_id)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT)
SORTKEY(artist_id)
""")

time_table_create = ("""
CREATE TABLE time(
start_time BIGINT PRIMARY KEY,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday VARCHAR)
SORTKEY(start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} iam_role {} FORMAT AS JSON {} region 'us-west-2'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {} iam_role {} FORMAT AS JSON 'auto' region 'us-west-2'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT e.ts,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent
  FROM staging_events e
  JOIN staging_songs s
    ON e.artist = s.artist_name
   AND e.song = s.title
   AND e.page LIKE 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId,
       firstName,
       lastName,
       gender,
       level
  FROM staging_events
 WHERE userId IS NOT NULL
   AND page LIKE 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (
SELECT DISTINCT song_id,
       title,
       artist_id,
       year,
       duration
  FROM staging_songs
 WHERE song_id IS NOT NULL)
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
  FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT ts,
       EXTRACT(HOUR FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS hour,
       EXTRACT(DAY FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS day,
       EXTRACT(WEEKS FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS week,
       EXTRACT(MONTH FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS month,
       EXTRACT(YEAR FROM TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second') AS year,
       to_char(TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second', 'Day') AS weekday
  FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert]
