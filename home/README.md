# Project Summary
Create a Postgres database optimized to support song play analysis including the creation of the a database schema and ETL pipeline for the Sparkify analytics team.The included python scripts enable the user to create a star schema database of song plays for their analysis purposes. The data is from http://millionsongdataset.com/ and is in JSON format.

The database schema for the sparkifydb database ...
Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id
    - start_time foreign key time table
    - user_id foreign key time table
    - level
    - song_id foreign key
    - artist_id foreign key
    - session_id
    - location
    - user_agent

Dimension Tables
- users - users in the app
    - user_id
    - first_name
    - last_name
    - gender
    - level

- songs - songs in music database
    - song_id
    - title
    - artist_id foreign key artist table
    - year
    - duration

- artists - artists in music database
    - artist_id
    - name
    - location
    - lattitude
    - longitude

- time - timestamps of records in songplays broken down into specific units
    - start_time
    - hour
    - day
    - week
    - month
    - year
    - weekday
    
# Explanation of Files in Repository

## sql_queries.py
Contains the create, insert, and drop SQL statements for the above data schema.

## create_tables.py
Contains routines to creates the database, creates the tables, and drop the tables.

## etl.py
Countines the routines to process the JSON files and insert the data into the tables.

## data

### song data
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### log data
![Image of Log](https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)


# Process to Follow

To build your database named sparkifydb (userid = student and password = student) for songplay analysis ...

1. Place the json log files in /data/log_data (additional subdirectories are acceptable)d 
2. Place the json song files in /data/song_data (additional subdirectories are acceptable)
3. execute the command python create_tables.py
4. execute the command python etl.py

Once both scripts complete, the database is ready for querying, thus enabling the end user to performance analysis on song plays.
