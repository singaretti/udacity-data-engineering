class SqlQueries:
    
    songplay_table = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)
    
    user_table_append = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        AND userid NOT IN (SELECT userid FROM users)
    """)

    song_table = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    song_table_append = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id NOT IN (SELECT song_id FROM songs)
    """)

    artist_table = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)
    
    artist_table_append = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id NOT IN (SELECT artist_id FROM artists)
    """)

    time_table = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    time_table_append = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        WHERE start_time NOT IN (SELECT start_time FROM time)
    """)

    artist_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.artists (
               artistid varchar(256) NOT NULL,
               name varchar(256),
               location varchar(256),
               lattitude numeric(18,0),
               longitude numeric(18,0))
    """)

    artist_create_table_tmp = ("""
        CREATE TABLE IF NOT EXISTS public.artists_tmp (
               artistid varchar(256) NOT NULL,
               name varchar(256),
               location varchar(256),
               lattitude numeric(18,0),
               longitude numeric(18,0))
    """)
    
    song_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.songs (
               songid varchar(256) NOT NULL,
               title varchar(256),
               artistid varchar(256),
               "year" int4,
               duration numeric(18,0),
               CONSTRAINT songs_pkey PRIMARY KEY (songid))
    """)
    
    song_create_table_tmp = ("""
        CREATE TABLE IF NOT EXISTS public.songs_tmp (
               songid varchar(256) NOT NULL,
               title varchar(256),
               artistid varchar(256),
               "year" int4,
               duration numeric(18,0),
               CONSTRAINT songs_pkey_tmp PRIMARY KEY (songid))
    """)
    
    time_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.time (
               start_time timestamp NOT NULL,
               "hour" int4,
               "day" int4,
               week int4,
               "month" varchar(256),
               "year" int4,
               weekday varchar(256),
               CONSTRAINT time_pkey PRIMARY KEY (start_time))
    """)
    
    time_create_table_tmp = ("""
        CREATE TABLE IF NOT EXISTS public.time_tmp (
               start_time timestamp NOT NULL,
               "hour" int4,
               "day" int4,
               week int4,
               "month" varchar(256),
               "year" int4,
               weekday varchar(256),
               CONSTRAINT time_pkey_tmp PRIMARY KEY (start_time))
    """)
    
    user_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.users (
               userid int4 NOT NULL,
               first_name varchar(256),
               last_name varchar(256),
               gender varchar(256),
               "level" varchar(256),
               CONSTRAINT users_pkey PRIMARY KEY (userid))
    """)
    
    user_create_table_tmp = ("""
        CREATE TABLE IF NOT EXISTS public.users_tmp (
               userid int4 NOT NULL,
               first_name varchar(256),
               last_name varchar(256),
               gender varchar(256),
               "level" varchar(256),
               CONSTRAINT users_pkey_tmp PRIMARY KEY (userid))
    """)
    
    songplay_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.songplays (
               playid varchar(32) NOT NULL,
               start_time timestamp NOT NULL,
               userid int4 NOT NULL,
               "level" varchar(256),
               songid varchar(256),
               artistid varchar(256),
               sessionid int4,
               location varchar(256),
               user_agent varchar(256),
               CONSTRAINT songplays_pkey PRIMARY KEY (playid))
    """)