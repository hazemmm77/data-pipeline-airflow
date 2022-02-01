songplay_table_insert = ("""
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

user_table_insert = ("""INSERT INTO users(userid, first_name,last_name ,gender ,level )
                     select userId, firstName,lastName,gender,level
                     from staging_events where  userId is NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (songid,title,artistid,year,duration)
                    select   song_id,title,artist_id,year,duration
                    from staging_songs
""")

artist_table_insert = ("""INSERT INTO artists(artistid,name,location ,lattitude,longitude)
                       select artist_id,artist_name,artist_location,
                       artist_latitude,artist_longitude
                       from  staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                    SELECT    DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                    extract(hour from start_time),
                     extract(day from start_time),extract(week from  start_time)
                     ,extract(month from  start_time),extract(year from start_time)
                     ,extract(weekday from start_time) from staging_events
""")