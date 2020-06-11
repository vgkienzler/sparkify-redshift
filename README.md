
#### Query example: Get the title and artist name of the 50 most popular songs: 
SELECT A.name AS Artiste_Name, S.title AS Song_title, P.song_id, count(P.song_id) AS popularity<br />
	FROM songplays AS P<br />
    JOIN songs AS S on S.song_id = P.song_id<br />
    JOIN artists AS A on A.artist_id = S.artist_id<br />
    GROUP BY P.song_id, A.name, S.title<br />
    ORDER BY popularity DESC<br />
LIMIT 50;<br />

#### Query to get the total number of row in table `songplays`:
SELECT COUNT(*) FROM songplays<br />

#### Query to get the user id, first name, last name and gender of users who registered both as free and paid users:
SELECT user_id, first_name, last_name, gender, COUNT(user_id) AS number<br />
	FROM users<br />
    GROUP BY user_id, first_name, last_name, gender<br />
    ORDER by number DESC<br />