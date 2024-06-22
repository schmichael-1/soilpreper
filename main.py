import sqlite3
import pandas as pd
import os
from datetime import datetime
import pytz


def main():
    quit = True
    
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
    
    while quit:
        print(f'Options:')
        print(f'1. Load Data')
        print(f'2. Display Samples')
        
        user_input = input('')
        
        if user_input == f'1': # Load Data
            load_data()
            
        elif user_input == f'2': # View samples 
            print(f'Display:')
            print(f'1. Samples not in oven')    # Progress 0
            print(f'2. Samples in oven')        # Progress 1
            print(f'3. Samples milled')         # Progress 2
            print(f'4. Samples spun')           # Progress 3
            print(f'5. Samples complete')       # Progress 4
            print(f'6. All samples')
            sample_input = input()
            
            
            conn, cursor = open_connection()
            
            valid=True
            
            if sample_input == f'1':
                cursor.execute('''
                    SELECT sample_id, lab_loc, progress
                    FROM samples
                    WHERE progress=0
                ''')
                
            elif sample_input == f'2':
                cursor.execute('''
                    SELECT sample_id, lab_loc, progress
                    FROM samples
                    WHERE progress=1
                ''')
                
            elif sample_input == f'3':
                cursor.execute('''
                    SELECT sample_id, lab_loc, progress
                    FROM samples
                    WHERE progress=2
                ''')
                
            elif sample_input == f'4':
                cursor.execute('''
                    SELECT sample_id, lab_loc, progress
                    FROM samples
                    WHERE progress=3
                ''')
                
            elif sample_input == f'5':
                cursor.execute('''
                    SELECT sample_id, lab_loc, progress
                    FROM samples
                    WHERE progress=4
                ''')          
                
            elif sample_input == f'6':
                cursor.execute('''
                    SELECT sample_id, lab_loc, progress
                    FROM samples
                ''')
            else: valid = False                                                                                
            
            if valid:
                for row in cursor.fetchall():
                    print(row)
            
            close_connection(conn, cursor)
            
        elif user_input == f'd': # Drop tables
            conn, cursor = open_connection()
            cursor.execute('''DROP TABLE samples''')
            cursor.execute('''DROP TABLE files''')
            
            close_connection(conn, cursor)
        else: # Quit
            quit = False
            

            
        
    
    
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

def open_connection():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    return(conn, cursor)

def close_connection(conn, cursor):
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
