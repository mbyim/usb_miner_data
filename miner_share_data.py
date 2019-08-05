import pandas
import sqlite3
import csv
import time

sqlite3.enable_callback_tracebacks(True)
DB_NAME = "usb_miner_data.db"
SLOG_PATH = "/home/martin/git/vthoang/cgminer/shares.log"


def fix_timestamp(current_tstamp, row):
    """If length of timestamp is less than 10
    then we need to fix by appending current unix timestamp
    to the beginning of this (if only 6, append first 4 of current tstamp
    """
    timestamp = row[0]
    tlength = len(timestamp)
    if tlength < 10:
        fixed_tstamp = float(current_tstamp[:(10-tlength)] + timestamp)
        return [fixed_tstamp] + row[1:]
    else: 
        return [float(row[0])] + row[1:]  
   
      
def insert_to_db():
    """ Create table if not exists
    Insert new lines,
    make sure on conflict to do nothing!!
    Use max(timestamp) to determine what to insert
    """
    conn = sqlite3.connect(DB_NAME)
    #conn.set_trace_callback(print)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS shares (\
               timestamp UNIXEPOCH,\
               disposition TEXT,\
               target TEXT,\
               pool TEXT,\
               dev TEXT,\
               thr TEXT,\
               sharehash TEXT,\
               sharedata TEXT);") 
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tstamp_sharehash ON shares(timestamp, sharehash);")
    fieldnames = ['timestamp', 'disposition', 'target', 'pool', 'dev', 'thr', 'sharehash', 'sharedata']
    rows = [] 
    with open(SLOG_PATH, 'r') as shares:
         reader = csv.reader(shares, delimiter=",")
         for line in reader:
             rows.append(line)
    #Fix timestamps and insert
    print(len(rows))
    data = [tuple(fix_timestamp(str(time.time()), row)) for row in rows]
    print(data[1:3])
    print(len(data))
    c.executemany("INSERT OR IGNORE INTO shares(timestamp, disposition, target, pool, dev, thr, sharehash, sharedata) VALUES(?,?,?,?,?,?,?,?)", data)
    print("Inserted")
    
    conn.commit()
    conn.close()

def main():
    insert_to_db()
    


if __name__=="__main__":
   main()
