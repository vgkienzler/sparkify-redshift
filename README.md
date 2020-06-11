
#### Query example: Get the title and artist name of the 50 most popular songs: 

SELECT A.name AS Artiste_Name, S.title AS Song_title, P.song_id, count(P.song_id) AS popularity<br />
	FROM songplays AS P<br />
    JOIN songs AS S on S.song_id = P.song_id<br />
    JOIN artists AS A on A.artist_id = S.artist_id<br />
    GROUP BY P.song_id, A.name, S.title<br />
    ORDER BY popularity DESC<br />
LIMIT 50;<br />


### Query to get the total number of row in table `songplays`:
SELECT COUNT(*) FROM songplays<br />
