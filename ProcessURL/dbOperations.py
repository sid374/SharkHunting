from pymongo import MongoClient, ASCENDING, errors
import pdb
import logging

client = MongoClient()
db = client.shark_hunting
logger = logging.getLogger('sharkHunting.dbOperations')

def setupDb():
	'''
		Sets up the collections and indexes for our mongodb instance
	'''
	#TODO: make sure unique index is sufficient. I can see some rare loopholes right now
	db.stockCollection.create_index([
								("accessionNumber", ASCENDING),
                               ], unique=True, name="uniqueIndex")
	db.stockCollection.create_index([
								("stocks.cusip", ASCENDING),
                               ])
	db.stockCollection.create_index([
								("stocks.value", ASCENDING),
                               ])
	db.stockCollection.create_index([
								("cik", ASCENDING),
                               ])
	db.stockCollection.create_index([
								("tableTotalValue", ASCENDING),
                               ])

def insertStockIntoDb(stockDocument):
	'''
		stock: dictionary containing stock data
		This function inserts this entry into the stocks table of the database
	'''
	headerValidationKeys = ['cik', 'periodOfReport']#'cusip' , 'value'
	stockValidationKeys = ['cusip' , 'value']
	for key in headerValidationKeys:
		if key not in stockDocument.keys():
			logger.error('Header key validation failed') 
			return

	for stock in stockDocument['stocks']:
		for key in stockValidationKeys:
			if key not in stock.keys():
				logger.error('Stock key validation failed') 
				return
	stockCollection = db.stockCollection
	try:
		stockId = stockCollection.insert_one(stockDocument).inserted_id
	except errors.DuplicateKeyError as e:
		logger.info('Duplicate found while inserting to db, terminating insertion ')
		return False
	else:
		return True



#for flat schema, not in use right now
def setupDb2():
	'''
		Sets up the collections and indexes for our mongodb instance
	'''
	#TODO: make sure unique index is sufficient. I can see some rare loopholes right now
	db.stockCollection.create_index([
								("accessionNumber", ASCENDING),
								("cusip", ASCENDING),
                             	("value", ASCENDING),
                             	("sshPrnamt", ASCENDING),
                             	("sshPrnamtType", ASCENDING),
                             	("investmentDiscretion", ASCENDING),
                             	("putCall", ASCENDING),
                             	("otherManager", ASCENDING),
                               ], unique=True, name="uniqueIndex")
	db.stockCollection.create_index([
								("cusip", ASCENDING)
                               ])
	db.stockCollection.create_index([
								("cik", ASCENDING),
								("form13FFileNumber", ASCENDING),
								("value", ASCENDING)
                               ])
	db.stockCollection.create_index([
								("value", ASCENDING),
                               ])

#for flat schema not in use right now
def insertStockIntoDb2(stockDocument):
	'''
		stock: dictionary containing stock data
		This function inserts this entry into the stocks table of the database
	'''
	validationKeys = ['cusip', 'cik', 'periodOfReport', 'value']
	for key in validationKeys:
		if key not in stockDocument.keys():
			logger.error('Key validation failed') 
			return
	stockCollection = db.stockCollection
	try:
		stockId = stockCollection.insert_one(stockDocument).inserted_id
	except errors.DuplicateKeyError as e:
		logger.info('Duplicate found while inserting to db, terminating insertion ')
		return False
	else:
		return True