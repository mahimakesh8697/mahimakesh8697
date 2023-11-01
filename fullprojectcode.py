import pymongo
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import sqlite3         






#######connecting youtube Api########

def Api_connect():
    Api_Id="AIzaSyB7nEkVfzB0WBqTgrlgQCEiD_EpaSMC_lU"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#######list of channel ids##########
##########channelid--used=[['UCY6KjrDBN_tIRFT_QNqQbRQ','UCICJPXKv03--uQrQ2DQlfaQ',
# 'UCl1IvP5EGFcO02Ut2SZBh-g','UCtYLUTtgS3k1Fg4y5tAhLbw','UCL9wK9vQjmgyx7jGt20ZOkg','UCEd1NrdpIriOwr_ElH8pppg',
# 'UCugG6-k5QGbq_iDEPAnG4NQ','UCk3JZr7eS3pg5AGEvBdEvFg','UCWXbQxj-oE8blIOTBWyiz5g']]"UCV76gTiuLGg2mNKpK2n9M_w"



##############getting channel information using channel_id############



def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data



###########getiing  playlist ids using channel ids##############


def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data
    
######################getting video ids using channel id##########################


def get_channel_videos(channel_id):
    video_ids = []
    
    
    #get Uploads playlist id#
    
    
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


##############get video information using video ids############
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data




###########get comment information using video ids############


def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information


###########MongoDB Connection###############


client = pymongo.MongoClient("mongodb+srv://makeshk349:mahimahi12@cluster0.t0lvtsh.mongodb.net/")
db = client["Youtube_data"]

############# upload to MongoDB  ###########

def channel_details(channel_id):
    chan_details = get_channel_info(channel_id)
    playlist_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    video_details = get_video_info(vi_ids)
    comment_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":chan_details,"playlist_information":playlist_details,"video_information":video_details,
                     "comment_information":comment_details})
    
    return "uploaded done"




    

##############################################################################################################################################################




import pymongo
import sqlite3

########3Connect to MongoDB############
mongo_client = pymongo.MongoClient("mongodb+srv://makeshk349:mahimahi12@cluster0.t0lvtsh.mongodb.net/")
mongo_db = mongo_client["Youtube_data"]
my_collection = mongo_db["channel_details"]


##########3Define SQLite database connection###########3
conn = sqlite3.connect('youtube_data.db')
cursor = conn.cursor()


#############Create and populate channels table##############
try:
    cursor.execute('''
      CREATE TABLE IF NOT EXISTS channels (
        Channel_Id TEXT PRIMARY KEY,
        Channel_Name TEXT,
        Subscription_Count INTEGER,
        Views INTEGER,
        Total_Videos INTEGER,
        Channel_Description TEXT,
        Playlist_Id TEXT
      )
    ''')

    inserted_count = 0  # Variable to track the number of inserted rows

    for document in my_collection.find():
        channel_information = document.get('channel_information', {})
        
        # Extract data from the nested structure
        channel_id = channel_information.get('Channel_Id', '')
        channel_name = channel_information.get('Channel_Name', '')
        subscription_count = channel_information.get('Subscription_Count', 0)
        views = channel_information.get('Views', 0)
        total_videos = channel_information.get('Total_Videos', 0)
        channel_description = channel_information.get('Channel_Description', '')
        playlist_id = channel_information.get('Playlist_Id', '')

        cursor.execute("INSERT INTO channels (Channel_Id, Channel_Name, Subscription_Count, Views, Total_Videos, Channel_Description, Playlist_Id) VALUES (?, ?, ?, ?, ?, ?, ?)", (channel_id, channel_name, subscription_count, views, total_videos, channel_description, playlist_id))
        inserted_count += 1

    conn.commit()

    if inserted_count > 0:
        print(f"{inserted_count} rows inserted into the 'channels' table.")
    else:
        print("No data inserted into the 'channels' table.")

except sqlite3.Error as e:
    print(f"SQLite error: {e}")

finally:
    conn.close()
##########################################################################################################################################################################






import pymongo
import sqlite3
import pandas as pd

#############Connect to MongoDB####################

mongo_client = pymongo.MongoClient("mongodb+srv://makeshk349:mahimahi12@cluster0.t0lvtsh.mongodb.net/")
mongo_db = mongo_client["Youtube_data"]
my_collection = mongo_db["channel_details"]

########## Retrieve data from MongoDB and convert it to a Pandas DataFrame#############
data = list(my_collection.find())
df = pd.DataFrame(data)


########Define SQLite database connection########3
conn = sqlite3.connect('youtube_data.db')
cursor = conn.cursor()

#############Create and populate playlists table###########
try:
    cursor.execute('''
      CREATE TABLE IF NOT EXISTS playlists (
        PlaylistId TEXT PRIMARY KEY,
        Title TEXT,
        ChannelId TEXT,
        ChannelName TEXT,
        PublishedAt TIMESTAMP,
        VideoCount INTEGER
      )
    ''')

    insert_query_playlists = '''
        INSERT OR IGNORE INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
        VALUES (?, ?, ?, ?, ?, ?)
    '''

    inserted_count = 0  ###Variable to track the number of inserted rows##

    #################Iterate through the DataFrame and insert data into the SQLite database###############

    for index, row in df.iterrows():
        playlist_info_list = row.get('playlist_information', [])

        for playlist_info in playlist_info_list:
            playlist_id = playlist_info.get('PlaylistId', '')  # Access 'PlaylistId' from 'playlist_information'
            title = playlist_info.get('Title', '')  # Access 'Title' from 'playlist_information'
            channel_id = playlist_info.get('ChannelId', '')  # Access 'ChannelId' from 'playlist_information'
            channel_name = playlist_info.get('ChannelName', '')  # Access 'ChannelName' from 'playlist_information'
            published_at = playlist_info.get('PublishedAt', '')  # Access 'PublishedAt' from 'playlist_information'
            video_count = playlist_info.get('VideoCount', 0)  # Access 'VideoCount' from 'playlist_information'

            cursor.execute(insert_query_playlists, (playlist_id, title, channel_id, channel_name, published_at, video_count))
            inserted_count += 1

    conn.commit()

    if inserted_count > 0:
        print(f"{inserted_count} rows inserted into the 'playlists' table.")
    else:
        print("No data inserted into the 'playlists' table.")

except sqlite3.Error as e:
    print(f"SQLite error: {e}")

finally:
    conn.close()










#########################################################################################################################################################################################

import pymongo
import sqlite3

#####Connect to MongoDB###########

mongo_client = pymongo.MongoClient("mongodb+srv://makeshk349:mahimahi12@cluster0.t0lvtsh.mongodb.net/")
mongo_db = mongo_client["Youtube_data"]
my_collection = mongo_db["channel_details"]

#######3Retrieve data from MongoDB#######
data = list(my_collection.find())

#########Define SQLite database connection########
conn = sqlite3.connect('youtube_data.db')
cursor = conn.cursor()

##########Create and populate videos table#############
try:
    cursor.execute('''
      CREATE TABLE IF NOT EXISTS videos (
        Channel_Id TEXT,
        Video_Id TEXT PRIMARY KEY,
        Title TEXT,
        Tags TEXT,
        Thumbnail TEXT,
        Description TEXT,
        Published_Date TIMESTAMP,
        Duration INTERVAL,
        Views INTEGER,
        Likes INTEGER,
        Comments INTEGER,
        Favorite_Count INTEGER,
        Definition TEXT,
        Caption_Status TEXT
      )
    ''')

    insert_query_videos = '''
        INSERT OR IGNORE INTO videos (Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    inserted_count = 0  ##Variable to track the number of inserted rows#######

    #########Iterate through the data and insert it into the SQLite database#########

    for row in data:
        video_info_list = row.get('video_information', [])
        for video_info in video_info_list:
            channel_id = video_info.get('channel_id', '')
            video_id = video_info.get('video_id', '')
            title = video_info.get('title', '')
            tags = ', '.join(video_info.get('tags', []))
            thumbnail = video_info.get('thumbnail', '')
            description = video_info.get('description', '')
            published_date = video_info.get('published_date', '')
            duration = video_info.get('duration', '')
            views = int(video_info.get('views', 0))
            likes = int(video_info.get('likes', 0))
            comments = int(video_info.get('comments', 0))
            favorite_count = int(video_info.get('favorite_count', 0))
            definition = video_info.get('definition', '')
            caption_status = video_info.get('caption_status', '')

            cursor.execute(insert_query_videos, (
                channel_id, video_id, title, tags, thumbnail, description, published_date, duration, views, likes, comments, favorite_count, definition, caption_status
            ))
            inserted_count += 1

    conn.commit()

    if inserted_count > 0:
        print(f"{inserted_count} rows inserted into the 'videos' table.")
    else:
        print("No data inserted into the 'videos' table.")

except sqlite3.Error as e:
    print(f"SQLite error (videos): {e}")

finally:
    conn.close()
##################################################################################################################################################################################


import pymongo
import sqlite3
import pandas as pd
import json

##############Connect to MongoDB####################
mongo_client = pymongo.MongoClient("mongodb+srv://makeshk349:mahimahi12@cluster0.t0lvtsh.mongodb.net/")
mongo_db = mongo_client["Youtube_data"]
my_collection = mongo_db["channel_details"]

#############3Retrieve data from MongoDB and convert it to a Pandas DataFrame##############
data = list(my_collection.find())
df = pd.DataFrame(data)

#########3Define SQLite database connection########3
conn = sqlite3.connect('youtube_data.db')
cursor = conn.cursor()

############Create and populate comments table###################
try:
    cursor.execute('''
      CREATE TABLE IF NOT EXISTS comments (
        Comment_Id TEXT PRIMARY KEY,
        Video_Id TEXT,
        Comment_Text TEXT,
        Comment_Author TEXT,
        Comment_Published TIMESTAMP
      )
    ''')

    insert_query_comments = '''
        INSERT OR IGNORE INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
        VALUES (?, ?, ?, ?, ?)
    '''

    inserted_count = 0  ###Variable to track the number of inserted rows###

    ##########Iterate through the DataFrame and insert data into the SQLite database#############

    for index, row in df.iterrows():
        comment_id = str(row['_id'])  # Assuming '_id' is the Comment_Id

        # Access the list 'comment_information'
        comment_information = row.get('comment_information', [])

        if comment_information:
            for info in comment_information:
                video_id = info.get('video_id', '')  # Access the 'video_id' field within the list
                comment_text = json.dumps(info.get('comment_text', {}))  # Access the 'comment_text' field within the list
                comment_author = info.get('comment_author', '')  # Access the 'comment_author' field within the list
                comment_published = info.get('comment_published', '')  # Access the 'comment_published' field within the list

                cursor.execute(insert_query_comments, (comment_id, video_id, comment_text, comment_author, comment_published))
                inserted_count += 1

    conn.commit()

    if inserted_count > 0:
        print(f"{inserted_count} rows inserted into the 'comments' table.")
    else:
        print("No data inserted into the 'comments' table.")

except sqlite3.Error as e:
    print(f"SQLite error: {e}")

finally:
    conn.close()
#####################################################################################################################################################################

#####################################################################################################################################################################






import pymongo
import streamlit as st
import pandas as pd
import sqlite3


##############Connect to MongoDB###########
client = pymongo.MongoClient("mongodb+srv://makeshk349:mahimahi12@cluster0.t0lvtsh.mongodb.net/")
db = client["Youtube_data"]
coll1 =db["channel_details"]


mydb = sqlite3.connect('youtube_data.db')
cursor = mydb.cursor()


#############USING THIS FUNCTION FOR REFERENCE -WHICH IS ALREADY USED ############
def channel_details(channel_id):
    chan_details = get_channel_info(channel_id)
    playlist_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    video_details = get_video_info(vi_ids)
    comment_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":chan_details,"playlist_information":playlist_details,"video_information":video_details,
                     "comment_information":comment_details})
    
    return "uploaded done"

import sqlite3

############################DISPLAYING-TABLES#############################

def display_channels_table():
    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM channels")
    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        print(row)

def display_playlists_table():
    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM playlists")
    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        print(row)

def display_videos_table():
    conn = sqlite3 .connect('youtube_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM videos")
    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        print(row)

def display_comments_table():
    conn = sqlite3.connect('youtube_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM comments")
    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        print(row)

def display_all_tables():
    display_channels_table()
    display_playlists_table()
    display_videos_table()
    display_comments_table()
    return "Tables Created successfully"

##########Call the 'display_all_tables()' function to show the data###########
##############display_all_tables()##########




#########################defining functions for showing table in streamlit app###########

def view_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table




def view_playlists_table():
    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    play_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                play_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(play_list)
    return playlists_table

def view_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def view_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table




#############CREATING -KEYS AND BUTTONS IN STREAMLIT#################
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILLS APPLIED IN THIS PROJECT")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB as DATALAKE")
    st.caption("API Integration IN YOUTUBE")
    st.caption(" Data Managment using MongoDB and SQL")
    st.caption("SQL-AS DATAWAREHOUSE")
    st.caption("USING STREAMLIT TO CREATE AND DISPLAY AN USER INTREFACE")
    st.caption("SOLVING QUESTIONS USING SQL-QUERY AND DISPLAY IT IN UI")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output =channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = display_all_tables()
    st.success(display)
    
show_table = st.radio("WHICH TABLE YOU WANT TO VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    view_channels_table()
elif show_table == ":orange[playlists]":
    view_playlists_table()
elif show_table ==":red[videos]":
    view_videos_table()
elif show_table == ":blue[comments]":
    view_comments_table()


question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

     
if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 =query8 = '''
    SELECT Title AS Video_Title, Channel_Name, 
           strftime('%Y-%m-%d', datetime(Published_Date, 'unixepoch')) AS Published_Date
    FROM videos
    WHERE strftime('%Y', datetime(Published_Date, 'unixepoch')) = '2022';
'''

    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))


