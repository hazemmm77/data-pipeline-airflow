## Description
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
the analytics team is particularly interested in understanding what songs users are listening to.
 I create a database schema and ETL pipeline using Airflow for this analysis to be optimized for queries on song play analysis.
 Using the song and log datasets

 ## Database Design
 ### Fact Table
   **songplays** - records in log data associated with song plays i.e. records with page NextSong
   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

### Dimension Tables
  **users** - users in the app
  _user_id, first_name, last_name, gender, level_

  **songs** - songs in music database
   _song_id, title, artist_id, year, duration_

  **artists** - artists in music database
  _artist_id, name, location, latitude, longitude_

  **time** - timestamps of records in songplays broken down into specific units
  _start_time, hour, day, week, month, year, weekday_

  ## ETL Process
   build an ETL pipeline using Python . reading data from log files  from directories data\song_data and data\log_data,insert them into staging tables  in Redshift then from staging table to fact table and Dimension tables

  ## Project  files
  ### dags folder
  1.sql_quieres.py : contains all sql quieres i needed in project
  2.udac_example.py: contains the main code for Dag i used in my project
  ### pligins folder--operation folder
  1.data_quality.py   implement of DataQualityOperator  to check quality of the data ( usesd in Dag)
  2. loaddminsions.py   implement of LoadDimensionOperator to load data from staging tables to dimention tables(used in Dag)
  3. loadfact.py         implement of LoadFactOperator to load data from staging tables to fact table(used in Dag)
  4. stage_redshift.py   implement of StageToRedshiftOperato to load data from s3 backet to staging tables(used in Dag)
  ## ```README.md``` provides discussion on your project.
2.
 ## How To Run the Project
 run from Airflow according to schedule


