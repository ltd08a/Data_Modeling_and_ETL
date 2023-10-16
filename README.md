# Data Modeling and ETL with Postgres and Python

### Project Description

The purpose of this project was to take raw data from Sparkify and model it into a form that can be easily used for analysis. There were two raw JSON files used to draw data from. I had to build an ETL pipeline to get the data that was needed to perform the analysis. I used a star schema when creating tables to make for more customizable queries. The work was done in Postgres using SQL and Python. 

### The Data

There were 2 JSON files used. The first was song_data, which contains various information about the songs from Sparkify. Some of the information includes artist names, title of the song, and duration. The second was log_data, which had information such as user names, time stamps, and subscription level. 

### Creating The Schema

I chose a star schema for this project. This will allow more flexibility for queries. My fact table (songplays) contains data such as songplay_id, artist_id, song_id, timestamp, and user_id to link it to the dimension tables. The dimension tables used were artists, songs, time, and users. All of the dimensional tables have a primary key derived from the fact table. 

![image info]("C:\Users\logan\OneDrive\Desktop\WGU\Data Wrangling\Screenshot 2023-10-16 144614.png")
