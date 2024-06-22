import sqlite3
import pandas as pd
import os
from datetime import datetime
import pytz


def main():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY,
            sample_id TEXT NOT NULL,
            lab_loc TEXT NOT NULL,
            progress INTEGER
        ) 
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            file INTEGER NOT NULL,
            time TEXT NOT NULL,
            parsed INTEGER NOT NULL
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    
    load_data()
    
    
def load_data(): # Checks data\ for any new .xlsx files, and extracts samples from files that are not parsed yet
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        DROP TABLE IF EXISTS files_temp
    ''')
    cursor.execute('''
        CREATE TABLE files_temp(
                       file TEXT NOT NULL
        )
    ''')
        
    files = os.listdir('data')
    
    for x in files:
        if x[-5:] == '.xlsx':
            cursor.execute('''
               INSERT INTO files_temp (file) VALUES (?) 
            ''', (x,))
    
    cursor.execute('''
        SELECT files_temp.file
        FROM files_temp
        LEFT JOIN files ON files.file = files_temp.file
        WHERE files.file IS NULL
    ''')
    
    for row in cursor.fetchall():
        tz_SA = pytz.timezone('Australia/Adelaide')
        datetime_SA = datetime.now(tz_SA)
        datetime_SA = datetime_SA.strftime('%d/%m/%y %H:%M:%S')
        
        
        cursor.execute(
            '''INSERT INTO files (file, time, parsed) VALUES (?,?,? )
            ''', (row[0],datetime_SA,0,)
        )
    
    conn.commit()
    
    cursor.execute('''
       SELECT files.file
       FROM files
       WHERE parsed IS 0 
    ''')    
    
    for row in cursor.fetchall():
         data = pd.read_excel(f'data\{row[0]}', header=None)
         data = data.reset_index()
         
         for index, data_row in data.iterrows():
            cursor.execute('''
                INSERT INTO samples (sample_id, lab_loc, progress) VALUES (?, ?, 0)
                ''', (data_row[0], data_row[1]))

            cursor.execute('''
                UPDATE files SET parsed=1
                WHERE file = (?)
            ''', (row[0],))
        
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
