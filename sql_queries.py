import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN            =    config.get('IAM_ROLE', 'ARN')
LOG_DATA       =    config.get('S3', 'LOG_DATA')
LOG_JSONPATH   =    config.get('S3', 'LOG_JSONPATH')
SONG_DATA      =    config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# STAGING TABLES (READ DATA FROM FILES ON S3 AND STOREd IN TABLES)

staging_events_table_create= ("""
        
        CREATE TABLE staging_events (
            artist              VARCHAR,
            auth                VARCHAR,
            firstName           VARCHAR,
            gender              CHAR,
            itemInSession       VARCHAR,
            lastName            VARCHAR,
            length              VARCHAR,
            level               VARCHAR,
            location            VARCHAR,
            method              VARCHAR,
            page                VARCHAR,
            registration        FLOAT,
            sessionId           INT,
            song                VARCHAR,
            status              INT,
            ts                  BIGINT,
            userAgent           VARCHAR,
            userId              VARCHAR
        );
    
""")

staging_songs_table_create = ("""

        CREATE TABLE staging_songs (
            num_songs           INT,
            artist_id           VARCHAR,
            artist_latitude     FLOAT,
            artist_longitude    FLOAT,
            artist_location     VARCHAR,
            artist_name         VARCHAR,
            song_id             VARCHAR,
            title               VARCHAR,
            duration            FLOAT,
            year                INT
    );      
    
""")

# Fact Table

# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


songplay_table_create = ("""

    CREATE TABLE IF NOT EXISTS songplays (
    
        songplay_id INT         IDENTITY(0,1),
        start_time  TIME        NOT NULL REFERENCES time (start_time),
        user_id     INT         NOT NULL REFERENCES users (user_id),
        level       VARCHAR,
        song_id     VARCHAR     NOT NULL REFERENCES songs (song_id) distkey,
        artist_id   VARCHAR     NOT NULL REFERENCES artists (artist_id),
        session_id  INT,
        location    VARCHAR,
        user_agent  VARCHAR,
        PRIMARY KEY (songplay_id),
        CONSTRAINT  time_user_song_artist_key
        UNIQUE      (start_time, user_id, song_id, artist_id)    
    )
    sortkey (start_time, user_id, song_id, artist_id);
    
""")

user_table_create = ("""

    CREATE TABLE IF NOT EXISTS users (

        user_id      VARCHAR,
        first_name   VARCHAR,
        last_name    VARCHAR,
        gender       CHAR,
        level        VARCHAR,
        PRIMARY KEY (user_id)
        
    );

""")

song_table_create = ("""

    CREATE TABLE IF NOT EXISTS songs (

        song_id     VARCHAR,
        title       VARCHAR,
        artist_id   VARCHAR         REFERENCES artists (artist_id),
        year        INT,
        duration    FLOAT,
        PRIMARY KEY (song_id)
        );

""")

artist_table_create = ("""

    CREATE TABLE IF NOT EXISTS artists (

        artist_id   VARCHAR,
        name        VARCHAR,
        location    VARCHAR,
        latitude    FLOAT,
        longitude   FLOAT,
        PRIMARY KEY (artist_id)

        );
        
""")

time_table_create = ("""

    CREATE TABLE IF NOT EXISTS time (

        start_time TIME,
        hour       INT,
        day        INT,
        week       INT,
        month      INT,
        year       INT,
        weekday    VARCHAR,
        PRIMARY KEY (start_time)

        );

""")

# STAGING TABLES
# use JSON PATH define all columns in right order

staging_events_copy = ("""

    copy staging_events 
    from {}
    iam_role '{}'
    json {}
    region 'us-west-2';

""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""

     copy staging_songs
     from {}
     iam_role '{}'
     region 'us-west-2'
     json 'auto';
     
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""

    INSERT INTO songplay (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS start_time, userId as user_id, level, song_id, artist_id, sessionId as session_id, location, userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.artist = ss.artist_name and se.length = ss.duration and se.song = ss.title
    GROUP BY ts, userId, level, song_id, artist_id, sessionId, location, userAgent

""")

user_table_insert = ("""

    INSERT INTO user (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT userId, firstName, lastName, gender, level
    FROM staging_songs
""")

song_table_insert = ("""

    INSERT INTO song (
        song_id,
        title,
        artist_id ,
        year,
        duration  
    )
    SELECT song_id, title, artist_id, year, duration
    FROM staging_events
""")

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_events

""")

time_table_insert = ("""
    INSERT INTO time (
        start time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT  start_time, 
            EXTRACT(HOUR, start_time) AS hour, 
            EXTRACT(DAY, start_time) AS day, 
            EXTRACT(WEEK, start_time) AS week, 
            EXTRACT(MONTH,start_time) AS month, 
            EXTRACT(YEAR, start_time) AS year, 
            EXTRACT(WEEKDAY, start_time)  AS weekday
    FROM
    (
    
        SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time 
        FROM staging_events
        
    )AS a
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
