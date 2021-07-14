import os
import io
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
   Process one song file to load songs and artists tables.
        
        Parameters:
            cur: Cursor of the database Connection
            filepath: filepath of a song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)
    
def load_time(cur, df):
    """ 
    Extract and process the required data to load time table.
        
        Parameters:
            cur: Cursor of the database Connection
            df: dataframe after NextSong filter
    """
    # convert timestamp column to datetime
    t = pd.DataFrame({'ts':df['ts'], 'datetime':pd.to_datetime(df['ts'], unit='ms')}).drop_duplicates()
    
    time_data = {'start_time':t['ts'],'hour':t['datetime'].dt.hour, 'day':t['datetime'].dt.day, 'week':t['datetime'].dt.week,'month':t['datetime'].dt.month, 'year':t['datetime'].dt.year, 'wekday':t['datetime'].dt.weekday}
    time_df = pd.DataFrame(time_data)
    
    # save data into a StringIO
    time_output = io.StringIO()
    time_df.to_csv(time_output, index=False, header=False)
    time_output.seek(0)
    
    # drop temp table
    cur.execute(drop_temp_time)
    # create temp table temp_time
    cur.execute(create_temp_time)

    try:
        cur.copy_from(time_output, 'temp_time', sep=',')
        # insert time records from temp_time to time
        cur.execute(time_table_insert_from_temp)
    
    except (Exception, psycopg2.Error) as e:
        print(e)

def load_users(cur, df):
    """
    Extract and process the required data to load users table.
        
        Parameters:
            cur: Cursor of the database Connection
            df: dataframe after NextSong filter
    """
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # save data into a StringIO
    user_output = io.StringIO()
    user_df.to_csv(user_output, index=False, header=False)
    user_output.seek(0)
    
    # drop temp_users table
    cur.execute(drop_temp_users)
    # create temp table temp_users
    cur.execute(create_temp_users)

    try:
        #copy data from StringIO to temp_users t
        cur.copy_from(user_output, 'temp_users', sep=',')
        #insert records from temp_users to users
        cur.execute(user_table_insert_from_temp)
    except (Exception, psycopg2.Error) as e:
        print(e)
    
    
def load_songplays(cur, df):
    """ 
    Extract and process the required data to load songplays table.
        
        Parameters:
            cur: Cursor of the database Connection
            df: dataframe after NextSong filter
    """
    all_data = []
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        #cur.execute(song_select, (row.song, row.artist, row.length))
        #should just match song title and artist name
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        all_data.append(songplay_data)
        
        #insert songplay one by one
        #cur.execute(songplay_table_insert, songplay_data)
    
    # create df from all_data
    songplay_df = pd.DataFrame(all_data)
    songplay_df = songplay_df.drop_duplicates()
    # save to StringIO
    songplay_output = io.StringIO()
    
    # need to use '&' as delimiter as the user_agent field contains ','
    songplay_df.to_csv(songplay_output, index=False, header=False, sep='&')
    songplay_output.seek(0)
    
    # drop temp_songplays
    cur.execute(drop_temp_songplays)
    #create temp_songplays
    cur.execute(create_temp_songplays)
    
    # copy from songplay_output
    try:
        #copy from StringIO to temp_songplays
        cur.copy_from(songplay_output, 'temp_songplays ', sep='&')
        #insert into songplays from temp_songplays
        cur.execute(songplay_table_insert_from_temp)
    
    except (Exception, psycopg2.Error) as e:
        print(e)
    

def process_log_file(cur, filepath):
    """
    Process one log file to load time, users and songplays tables
    
        Parameters:
            cur: Cursor of the database Connection
            filepath: filepath of the processed file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # load time table
    load_time(cur, df)
    
    # load user table
    load_users(cur, df)

    # load songplay table
    load_songplays(cur, df)
    

def process_data(cur, conn, filepath, func):
    """
    Loop through all folder and sub folder of the path to process data on each file.
    
        Parameters:
                    cur: cursor of the connection
                    conn: connection to postgres database
                    filepath: folder path of the files
                    func: either process_song_file or process_logfile function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()