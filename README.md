# Data Modeling and ETL
### Project Description
The purpose of this project was to take raw data from Sparkify and model it into a form that can be easily used for analysis. There were two raw JSON files used to draw data from. I had to build an ETL pipeline to get the data that was needed to perform the analysis. I used a star schema when creating tables to make for more customizable queries.
### Environment(s)
Jupyter Notebook
### Language(s)
PostgreSQL  
Python
### Libraries and Packages
pandas  
psycopg2  
os  
glob  
sql_queries
### Competencies
1. Data Gathering: Conduct data extraction and wrangling with data in complex formats for parsing and scraping.
2. Data Quality Assessment and Cleaning: Ensure data cleanliness through auditing and intervention.
### The Data
There were 2 JSON files used. The first was song_data, which contains various information about the songs from Sparkify. Some of the information includes artist names, title of the song, and duration. The second was log_data, which had information such as user names, time stamps, and subscription level. 
### Creating The Schema
I chose a star schema for this project. This will allow more flexibility for queries. My fact table (songplays) contains data such as songplay_id, artist_id, song_id, timestamp, and user_id to link it to the dimension tables. The dimension tables used were artists, songs, time, and users. All of the dimensional tables have a primary key derived from the fact table. 

![image info](https://github.com/ltd08a/data-modeling-with-postgres/blob/main/Star-Schema.png)

### Files
create_files.py: A script used to drop and create tables. Run this script before the other scripts.  
etl.ipynb: This notebook contains the details of the ETL process.  
etl.py: Uses data from the etl.ipynb notebook to extract and insert data into the tables.  
sql_queries.py: A script containing all of the queries.  
test.ipynb: A notebook used to check that the data is being inserted and stored properly.  
Star-Schema.png: An ERD generated to show the relationship in the star schema.  
README.md: A file describing the details of the project.  
### Run The Scripts
1. Run the create_files.py script through the shell to generate the database and tables. 
2. Run the etl.py script through the shell to populate the data into the tables.
3. Run the code in the test.ipynb notebook to confirm the data has populated. 
