from pymongo import MongoClient, ASCENDING, errors

client = MongoClient()
db = client.shark_hunting

def setupDb():
	'''
		Sets up the collections and indexes for our mongodb instance
	'''
	db.stockCollection.create_index([
								("cusip", ASCENDING),
                             	("cik", ASCENDING),
                             	("periodOfReport", ASCENDING),
                             	("value", ASCENDING)
                               ], unique=True)
	db.stockCollection.create_index([
								("cusip", ASCENDING)
                               ])
	db.stockCollection.create_index([
								("cik", ASCENDING),
								("form13FFileNumber", ASCENDING),
								("value", ASCENDING)
                               ])

def insertStockIntoDb(stockDocument):
	'''
		stock: dictionary containing stock data
		This function inserts this entry into the stocks table of the database
	'''
	stockCollection = db.stockCollection
	try:
		stockId = stockCollection.insert_one(stockDocument).inserted_id
	except errors.DuplicateKeyError as e:
		print 'Duplicate found, terminating insertion'
		return False
	else:
		#print str(stockId) + 'Written to db'
		return True