# Sparkify AWS-Redshift ETL pipeline
___

## Project summary

This project builds an ETL pipeline between JSON files containing music-related data and a AWS-Redshift database.

Two types of JSON files are processed:
- Song files, which contain information about the songs such as artist information, length of the song, album, etc. and

- Login files, which contain information about the users and their session 

## 'sparkify' database

The database is structured as follows:

#### Fact Table:

- **songplays** - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables:

- **users** - users in the app
    - user_id, first_name, last_name, gender, level
- **songs** - songs in music database
    - song_id, title, artist_id, year, duration
- **artists** - artists in music database
    - artist_id, name, location, latitude, longitude
- **time** - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## How to run the python scripts

**IMPORTANT NOTE:** before launching any script please make sure that the file `aws-secret.cfg` is up-to-date with 
the credentials of your aws user. To do so:
1. Open file `aws-secret.template`
2. Update option `key` in section `AWS` with your aws user key
3. Update option `secret` in section `AWS with your aws secret key
4. Save and rename file to `aws-secret.cfg`
5. Make sure never to share these credentials with anyone

2 Options are available to run the scripts:

**Option 1:** run `main.py`. This will launch automatically:
1. `create_role_cluster.py`: create the role and redshift cluster.If the cluster doesn't exist 
yet it will be created. If the cluster is created the user will be asked whether to wait or quit 
the program since cluster creation can take long. In the case the user chooses to quit, the process can be resumed later simply by launching again `main.py` 
2. Once the role and cluster are created and available, the user is asked whether to continue with table creation and loading. This will launch 
`create_table.py` and then `etl.py`. Please note that this will drop *ALL EXISTING TABLES* before re-creating 
them and loading them with data extracted from the JSON files.

**Option 2:** Alternatively you can launch all the scripts manually, in the following order:
1. Launch `create_role_cluster.py`
2. Once the cluster is available (you will have to check yourself form the AWS dashboard) launch `create_table.py`
3. Once the tables are created launch `etl.py`

## Description of the files in the repository
- File `main.py`: this file is the main file which calls all the other files and organise the 
ETL process.

- File `create_role_cluster.py`: opens the connection with aws, creates the redshift cluster and 
role to allow remote connection. 

- File `create_tables.py`: deletes all the tables if they already exist and re-create them anew based on the
queries in `sql_queries.py`.

- File `sql_queries.py`: contains all the sql queries used in 'create_tables.py' and in 'etl.py'

- File `etl.py`: this file runs the queries to:
    1. Load the redshift staging tables with data from the JSON files
    2. Load the fact and dimension tables from the staging tables
    3. Remove duplicates in tables `users` and `artists`. 

- File `dwh.cfg`: contains information about the cluster (endpoint, role arn, name, db, etc.)

- File `aws-secret.template`: template for `aws-secret.cfg`. Must be updated and renamed following the 
instructions above.

## Query examples:

Get the title and artist name of the 50 most popular songs:

    SELECT  A.name AS Artiste_Name,
            S.title AS Song_title,
            P.song_id, count(P.song_id) AS popularity
        FROM songplays AS P
        JOIN songs AS S on S.song_id = P.song_id
        JOIN artists AS A on A.artist_id = S.artist_id
    GROUP BY P.song_id, A.name, S.title
    ORDER BY popularity DESC
    LIMIT 50;

Query to get the total number of row in table `songplays`:

    SELECT COUNT(*) FROM songplays

Query to get the user id, first name, last name and gender of users who registered both as free and paid users:
   
    SELECT user_id, first_name, last_name, gender, COUNT(user_id) AS number
        FROM users
        GROUP BY user_id, first_name, last_name, gender
        ORDER by number DESC