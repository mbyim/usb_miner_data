import pandas as pd
from sqlalchemy import create_engine
import json
import time
import sqlite3
import os
import requests

WALLET_ADDRESS = os.environ['WALLET_ADDRESS']
POOL_URL = "http://ckpool.org/users/"
TABLE_NAME = "pool_data"
DB_FILE = "usb_miner_data.db"



def get_pool_data(): 
	r = requests.get(POOL_URL + WALLET_ADDRESS) 
	tstamp = time.time()
	if r.status_code != 200:
		print("ERROR: could not ping pool endpoint")
	json_data = json.loads(r.text)
	print(tstamp)
	print(json_data)	
	print(type(json_data))
	return tstamp, json_data 


def clean_hashrate(hashrate_txt):
	fixed = float(hashrate_txt[:-1])
	if 'K' in hashrate_txt:
		return fixed*1000
	elif 'M' in hashrate_txt:
		return fixed*1000000
	elif 'G' in hashrate_txt:
		return fixed*1000000000
	elif 'T' in hashrate_txt:
		return fixed*1000000000000
	#else just return the small number, will be clearly an error
	else:
		return fixed

def insert_data(tstamp, json_data):
	#Create df
	df = pd.io.json.json_normalize(json_data)
	df['tstamp'] = tstamp
	#Clean df
	df[['hashrate1m', 'hashrate5m', 'hashrate1hr', 'hashrate1d', 'hashrate7d']] = \
		df[['hashrate1m', 'hashrate5m', 'hashrate1hr', 'hashrate1d', 'hashrate7d']] \
			.applymap(lambda x: clean_hashrate(x))
	df['worker'] = df['worker'].astype(str)
	#create table if not exists
	conn = sqlite3.connect(DB_FILE)
	c = conn.cursor()
	c.execute("CREATE TABLE IF NOT EXISTS {} ( \
			ID INTEGER PRIMARY KEY AUTOINCREMENT,\
			hashrate1m BIGINT NOT NULL,\
			hashrate5m BIGINT NOT NULL,\
			hashrate1hr BIGINT NOT NULL,\
			hashrate1d BIGINT NOT NULL,\
			hashrate7d BIGINT NOT NULL,\
			lastshare BIGINT NOT NULL,\
			workers INT NOT NULL,\
			shares BIGINT NOT NULL,\
			bestshare NUMERIC NOT NULL,\
			lns NUMERIC NOT NULL,\
			luck NUMERIC NOT NULL,\
			accumulated NUMERIC,\
			postponed NUMERIC,\
			herp NUMERIC NOT NULL,\
			derp NUMERIC NOT NULL,\
			worker TEXT,\
			tstamp UNIXEPOCH)".format(TABLE_NAME))
	conn.commit()
	conn.close()
	#insert data
	engine = create_engine('sqlite:////home/martin/code/usb_miner_data/{}'.format(DB_FILE))
	df.to_sql(TABLE_NAME, con=engine, if_exists='append',index=False)



def main():
	tstamp, json_data = get_pool_data()
	insert_data(tstamp, json_data)	
	print("Inserted Pool Data")
if __name__=='__main__':
	main()

