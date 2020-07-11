import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
JSONPATH = config.get('S3','LOG_JSONPATH')
USER = config.get('CLUSTER','DB_USER')
PASSWORD = config.get('CLUSTER','DB_PASSWORD')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table
                                (event_id bigint IDENTITY(0,1),
                                 artist varchar(255),
                                 auth varchar,
                                 firstName varchar(255),
                                 gender varchar(1),
                                 itemInSession int,
                                 lastName varchar,
                                 length double precision,
                                 level varchar(50),
                                 location varchar,
                                 method varchar(50),
                                 page varchar,
                                 registration  varchar,
                                 sessionId bigint,
                                 song varchar(255),
                                 status int,
                                 ts bigint,
                                 userAgent text,
                                 userId bigint);
                              """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
                                (artist_id varchar,
                                artist_latitude double precision,
                                artist_location varchar(512),
                                artist_longitude double precision,
                                artist_name varchar(255),
                                duration double precision,
                                num_songs int,
                                song_id varchar,
                                title varchar(512),
                                year int);
                              """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table
                        (songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
                         start_time timestamp REFERENCES time_table(start_time) sortkey,
                         user_id varchar REFERENCES user_table(user_id),
                         level varchar(50),
                         song_id varchar REFERENCES song_table(song_id) distkey,
                         artist_id varchar REFERENCES artist_table(artist_id),
                         session_id int NOT NULL,
                         location text,
                         user_agent text);
                         """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                     (user_id bigint NOT NULL,
                     first_name varchar(255) NOT NULL,
                     last_name varchar(255) NOT NULL,
                     gender varchar(1) NOT NULL,
                     level varchar(50),
                     PRIMARY KEY(user_id))
                     DISTSTYLE all;
                     """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
                    (song_id varchar distkey,
                    title varchar(512) NOT NULL,
                    artist_id varchar NOT NULL REFERENCES artist_table(artist_id) sortkey,
                    year double precision,
                    duration float NOT NULL,
                    PRIMARY KEY(song_id));
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table
                      (artist_id varchar NOT NULL sortkey,
                      name varchar(255) NOT NULL,
                      location varchar(512),
                      latitude double precision,
                      longitude double precision,
                      PRIMARY KEY(artist_id))
                      DISTSTYLE all;
                       """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                    (start_time timestamp NOT NULL sortkey,
                    hour int NOT NULL,
                    day int NOT NULL,
                    week int NOT NULL,
                    month int NOT NULL,
                    year int NOT NULL,
                    weekday int NOT NULL,
                    PRIMARY KEY(start_time))
                    DISTSTYLE all;
                    """)

# STAGING TABLES

staging_events_copy = f"COPY staging_events_table from {LOG_DATA}\
                        credentials 'aws_iam_role={ARN}'\
                        format as JSON {JSONPATH}\
                        compupdate off region 'us-west-2';"

staging_songs_copy = f"COPY staging_songs_table from {SONG_DATA}\
                       credentials 'aws_iam_role={ARN}'\
                       format as JSON 'auto'\
                       compupdate off region 'us-west-2';"

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (start_time,
                                                        user_id, level, song_id,
                                                        artist_id, session_id,
                                                        location, user_agent)
                             (SELECT DISTINCT
                                     TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
                                     e.userId, e.level, s.song_id, s.artist_id,
                                     e.sessionId, e.location, e.userAgent
                              FROM (staging_events_table as e JOIN staging_songs_table as s
                                    ON e.song=s.title AND e.artist=s.artist_name)
                              WHERE e.page='NextSong'
                              AND userId NOT IN (SELECT DISTINCT user_id FROM songplay_table sp
                                                WHERE sp.session_id=e.sessionId
                                                AND sp.user_id=e.userId
                                                AND sp.start_time=start_time));
                         """)

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                        (SELECT DISTINCT userId, firstName, lastName, gender, level
                         FROM staging_events_table
                         WHERE page = 'NextSong'
                         AND userId NOT IN (SELECT DISTINCT user_id FROM user_table));
                     """)

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration)
                        (SELECT DISTINCT song_id, title, artist_id, year, duration
                         FROM staging_songs_table
                         WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song_table));
                     """)

artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
                          (SELECT DISTINCT artist_id, artist_name, artist_location,
                                 artist_latitude, artist_longitude
                           FROM staging_songs_table
                           WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist_table));
                       """)

time_table_insert = ("""INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
                        (SELECT start_time,
                               EXTRACT(hr from start_time) AS hour,
                               EXTRACT(d from start_time) AS day,
                               EXTRACT(w from start_time) AS week,
                               EXTRACT(m from start_time) AS month,
                               EXTRACT(yr from start_time) AS year,
                               EXTRACT(dw from start_time) AS weekday
                         FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time
                              FROM staging_events_table)
                         WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time_table));
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
