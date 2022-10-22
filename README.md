# ETL_Spotify
The purpose of this project is to build a program that downloads the data from my Spotify account about what songs I listen to throughout the day and saves the data into a data base. 

Step 1 will be to make an API request and extract the data from Spotify
NOTE: Sicne today's unix timestamp in milliseconds, we need to convert yesterday's as well

Step 2 will be to transform the data (validate)
1. Check if the dataframe is empty
2. Identify the primary key to preform Primary Key Check --> (played_at) because it has timestamps
3. We will also add an id to each row in our data base for a second Primary Key Check, which ensures each record has it's own id
4. Check for nulls
5. Check that all timestamps are of yesterday's date

Step 3 will be to load the data to local database (using PostgreSQL)
