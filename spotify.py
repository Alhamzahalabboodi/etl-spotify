#Dependencies
import requests
import json
import csv
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import datetime
import sqlite3
import matplotlib.pyplot as plt
from pprint import pprint
from sqlalchemy import create_engine

# My Spotify info
# Make an api request to extract the data from Spotify
user_id = "Alhamzah.alabboodi"
url = "https://api.spotify.com/v1/me/player/recently-played?after="
TOKEN = "BQCX70q3crylBpXfqeItcvwyEI_5MK_LIKYlPPqJQ--mLw0Fn88yBBG-esmQTaPvrSDtFh2fQPdo5pvIx6kvVtgGhaeFpWFRE-9KG3u3tCoLZw33AMG237yNBm2i1qI37SVNd6_Tqy4XKy24zVQ8rCclRq4inikHNasGHWEs5KRSb8bcPn1iolU0Yv-eHzs"

# Step number 2 : transform (validate)
# Create a function that checks for validations (this is typically boolean)
def check_if_validate_data(df: pd.DataFrame)-> bool:
    # Check if the dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing excution")
        return False
    # Identify the primary key to preform Primary Key Check --> (played_at) because it has timestamps
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    # Check for nulls
    if df.isnull().value.any():
        raise Exception("Null valued found")
    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strftime(timestamps, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    
    return True 


# Step number 1 : extract 
if __name__ == "__main__":
    
    headers = {
        "Accept" : "application/json",
        "content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    #sicne today's unix timestamp in milliseconds, we need to convert yesterday's as well
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) *1000
    
    #preform the request
    request =  requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers = headers)
    response = request.json()

#extract what we are looking for (song name, artist name, played at) | we loop through the response to extract the items

song_name = []
artist_name = []
played_at_list = []
timestamps = []

for song in response["items"]:
    song_name.append(song["track"]["name"])
    artist_name.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

#now let us create a dataframe where we can see the lists we create in a table 

song_dic = {
            "song_name" : song_name,
            "artist_name" : artist_name,
            "played_at" : played_at_list,
            "timestamp" : timestamps,
            }
song_df = pd.DataFrame(song_dic)
song_df.to_csv("my_played_tracks_data.csv")
    
#Validate
if check_if_validate_data(song_df):
    print("Data is valid, proceed to Load stage")

# Load
engine = sqlalchemy.create_engine("postgresql://postgres:password@localhost:5432/my_played_tracks")

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRINT primary_key_constraint PRIMARY KEY (played_at)
)

"""

try:
    song_df.to_sql("my_played_tracks",engine, index=False, if_exists="append")
except:
    print("Data already exist in the database")

print("Closen database successfully")

