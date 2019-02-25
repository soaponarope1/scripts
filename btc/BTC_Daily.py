#!/usr/local/bin/python3.6
import MySQLdb
from binance.client import Client
import datetime as dt
import time

print("started")

api_key = 'KPl3Fcy1NTsaCftXXVm3T7OJ2kbQoIDKRgUqB5SW0soCktdVVMApxRDj1xjPryBR'
api_secret = 'cs5icXlIb9GuBaaQQgANkKPTywa9DREOYTVolKJLeJjVxTo4FzT7CNLVhPJY4LbD'
client = Client(api_key, api_secret)

trPrList = ['BTCUSDT']

for pair in trPrList:
	print('doing things')
	BTChistData = client.get_historical_klines(pair, Client.KLINE_INTERVAL_30MINUTE, '1 day ago EST')

	OPEN_PRICE = 1
	HIGH_PRICE = 2
	LOW_PRICE = 3
	VOLUME = 5
	CLOSE_TIME = 6
	COUNT = 8

	#count is total trades
	for submission in BTChistData:
		subTime = str(submission[CLOSE_TIME])[:-3]
		subTimeInt = int(subTime)
		myTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(subTimeInt))
		myTime2 = "'" + myTime + "'"
		print("connecting to DB " + myTime + " " + str(subTime))
		#database name
		db = MySQLdb.connect(host='localhost',
				user='root',
				passwd='',
				db='historical_data')
		#db cursor
		cur = db.cursor()
		print("connected " + submission[OPEN_PRICE])
		try:
			sql = "INSERT INTO btc_price VALUES (%s,%s,%s,%s,%s)" % (str(submission[OPEN_PRICE]), str(submission[HIGH_PRICE]), str(submission[LOW_PRICE]), str(submission[VOLUME]), myTime2)
			print(sql)
			cur.execute(sql)
			#cur.execute(sql, (submission[OPEN_PRICE]), str(submission[HIGH_PRICE]), str(submission[LOW_PRICE]), str(submission[VOLUME]), str(myTime))
			db.commit()
		except Exception as e:
			db.rollback()
			print("We fucked up " + str(e))
			db.close()
   
