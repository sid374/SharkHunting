#!/usr/bin/python
import requests
from lxml import etree, objectify
import sys
import urllib
import pdb
import datetime
from dbOperations import insertStockIntoDb, setupDb
import os.path
import re
import logging

logger = None

def initLogger():
	global logger
	logger = logging.getLogger('sharkHunting')
	logger.setLevel(logging.DEBUG)
	#create file handler 13F xml already exists
	fh = logging.FileHandler('getURL.log')
	fh.setLevel(logging.DEBUG)
	#create stream handler for console 
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	# create formatter and add it to the handlers
	formatter = logging.Formatter('%(asctime)s - %(funcName)s:%(lineno)d - %(levelname)s - %(message)s')
	fh.setFormatter(formatter)
	ch.setFormatter(formatter)
	# add the handlers to the logger
	logger.addHandler(fh)
	logger.addHandler(ch)


def downloadAndProcess13FFromIndex(year, qtr, CIK_list = ['1048445', '921669', '1040273', '1418814', '1336528', '1365341'], force = False):
	setupDb()
	downloadIndexFile(year, qtr)
	logger.info('Processing index file for ' + str(year) + 'Q ' + str(qtr))
	pathToIndexFile = getIndexFilePathForYearAndQuarter(year, qtr)
	baseURL = 'https://www.sec.gov/Archives/'
	dict = {}
	lines = open(pathToIndexFile).read().splitlines()

	for line in lines:
		if line.find('13F-HR') != -1:
			#uncomment following to look for specific cik's only, right now we download ALL 13-F's!
			#for CIK in CIK_list:
			#	if line.find(CIK) != -1:
			words = line.split()
			CIK = words[-3]
			dict.update({CIK:words[-1]});

	for CIK,URL in dict.items():
		fPath = get13FFilePathForYearAndQuarter(year, qtr, CIK)
		if force == False and os.path.isfile(fPath):
			logger.info('13F xml already exists, please use force=True to redownload, XMLparser skipped')
		else:
			try:
				abcabc = urllib.urlretrieve(baseURL + URL, fPath)
				parse13F(fPath)
			except Exception as e:
				logger.error(e, exc_info=True)
				pdb.set_trace()



def retrieveURL(url):
	'''
		url: complete http url
		Given a url, returns the entire content of url as TEXT
	'''
	r = requests.get(url)
	r.raise_for_status()
	return r.text.encode('utf-8')


def retrieveXMLFile(url, saveFilePath = False):
	'''
		url: complete http url of xml file
		saveFilePath: if provided downloads xml file to this path
		description: Given a url to any xml file, returns a etree object of this xml file, and optionally downloads it
	'''
	if url[-4:] != '.xml':
		raise ValueError("xml file expected but not found")

	xmlDoc = retrieveURL(url)

	if saveFilePath != False:
		with open(saveFilePath,'w') as outFile:
			outFile.write(xmlDoc)

	root = etree.fromstring(xmlDoc)
	return root

def get13FFilePathForYearAndQuarter(year, qtr, cik):
	return './Data_13F/' + str(year) + 'Q' + str(qtr) + '_' + cik + '.xml'

def getIndexFilePathForYearAndQuarter(year, qtr, fileType = "company.idx"):
	return './Index/' + str(year) + 'Q' + str(qtr) + fileType

def downloadIndexFile(year =  2017, qtr = 1, fileType = "company.idx", force = False):
	'''
		fileType: one of the following -> company.idx, master.idx, form.idx  [read https://www.sec.gov/edgar/indices/fullindex.htm for more info]
		force: If True the file is downloaded even if it already exists
		Description: given a year and qtr, downlads the file type specified from the edgar database

	'''
	downloadPath = getIndexFilePathForYearAndQuarter(year, qtr, fileType)
	if force == False:
		if os.path.isfile(downloadPath):
			logger.warn('Index file already exists, use force=True to redownload. ' + downloadPath)
			return

	baseURL = 'https://www.sec.gov/Archives/edgar/full-index/'
	fileContents = retrieveURL(baseURL + '/' + str(year) + '/QTR' + str(qtr) + '/' + fileType )
	with open(downloadPath, 'w') as outFile:
		outFile.write(fileContents)

def downloadIndexFilesInRange(beggining, end):
	'''
		beggining: beggining year
		end: ending year
		downloads all index files from beggining to end (all quarters) 
	'''
	for year in xrange(beggining,end+1):
		for i in xrange(1,5):
			logger.debug('Downloading ' + str(year) + 'Q' + str(i)) 
			downloadIndexFile(year, i)


def stripNamespaceFromTag(elem):
	'''
		Helper function: given an etree element strips the namespace from the tag for easier reading
	'''
	if '}' in elem.tag:
			elem.tag = elem.tag.split('}', 1)[1]  

def convertStrToIntIfPossible(s):
	'''
		Convert str to int if possible, otherwise return the str back
	'''
	try:
		ret = int(s)
	except ValueError:
		ret = s
	return ret;

def parseStockXML(xmlStockRoot, ns):
	'''	
		Helper function: Given a etree element and namespaces dict with the stock infotable as root, this function parses the stock
	'''
	#TODO: Extract specific fields instead of below, so that we can catch errors early
	stockInfo = {}
	for i in xmlStockRoot:
		stripNamespaceFromTag(i)
		if i.tag == 'shrsOrPrnAmt':
			stripNamespaceFromTag(i[0])
			stripNamespaceFromTag(i[1])
			stockInfo[i[0].tag.strip()] = convertStrToIntIfPossible(i[0].text.strip())
			stockInfo[i[1].tag.strip()] = convertStrToIntIfPossible(i[1].text.strip())
		elif i.tag != None and i.text != None:
			stockInfo[i.tag.strip()] = convertStrToIntIfPossible(i.text.strip())
	if len(stockInfo.keys()) < 4:
		logger.error('Error parsing stock xml table' + str(stockInfo))
		return False
	return stockInfo

def parse13F(pathToFile):
	'''
		Parses the .txt 13-f files that we download from the idx files above
	'''
	logger.debug('Parsing 13F File ' + pathToFile) 
	ns = {	
			'13f':'http://www.sec.gov/edgar/thirteenffiler',
			'infoTable':'http://www.sec.gov/edgar/document/thirteenf/informationtable'
		 }
	parser = etree.XMLParser(recover=True) #recovers from broken xml files
	with open(pathToFile, 'r') as file:
		doc = file.read()

		#the entire text file is not a well formed xml document (and causes the parser to stumble) 
		#so we need to extract the relevant xml snippets by doing the following
		xmlCoverPage = doc[doc.find("<edgarSubmission"):doc.find("</edgarSubmission ")] 
		if xmlCoverPage == '':
			logger.error(pathToFile + 'Could not be parsed. Manual intervention required')
			return

		xmlRootCoverPage = etree.fromstring(xmlCoverPage, parser)
		try:
			value = xmlRootCoverPage.find('.//13f:tableValueTotal',namespaces=ns).text
		except Exception as e:
			logger.error(e, exc_info=True)
			value = '0' #Making value 0, so that we get into bogus 13f condition

		if int(value) == 0:
			logger.error('Bogus 13F ' + pathToFile) 
			return

		commonData = {}
		commonData['tableTotalValue'] = int(value)
		commonData['submissionType'] = xmlRootCoverPage.find('.//13f:submissionType',namespaces=ns).text
		commonData['periodOfReport'] = datetime.datetime.strptime(xmlRootCoverPage.find('.//13f:periodOfReport',namespaces=ns).text, "%m-%d-%Y")
		commonData['form13FFileNumber'] = xmlRootCoverPage.find('.//13f:form13FFileNumber',namespaces=ns).text
		commonData['cik'] = xmlRootCoverPage.find('.//13f:cik',namespaces=ns).text
		commonData['fundName'] = xmlRootCoverPage.find('.//13f:filingManager/13f:name',namespaces=ns).text
		commonData['xmlFilePath'] = pathToFile
		accessionNumberMatch = re.search("ACCESSION NUMBER\D*([0-9]+-[0-9]+-[0-9]+)", doc)
		if  accessionNumberMatch == None:
			logger.error("Accession number not found in 13F. Aborting")
			return
		else:
			if accessionNumberMatch != None:
				commonData['accessionNumber'] = accessionNumberMatch.group(1)

		mstart = re.search('<.*informationTable', doc)
		mend = re.search('</.*informationTable', doc)
		if mstart == None or mend == None:
			logger.error('Unable to parse XML for 13F '+ pathToFile + ' Manual intervention required') 
			return
		xmlStockTable = doc[mstart.start(0):mend.end(0)]
		xmlRootStockTable = etree.fromstring(xmlStockTable, parser)
		stockList = xmlRootStockTable.findall('.//infoTable:infoTable', namespaces=ns)
		stockDocumentList = []
		for stock in stockList:
			stockDocument = parseStockXML(stock, ns)
			if stockDocument == False:
				return
			#stockDocument.update(commonData)
			stockDocument['percentValueOfPortfolio'] = float(stockDocument['value']) / float(value)
			stockDocumentList.append(stockDocument)
		commonData['stocks'] = stockDocumentList
		if insertStockIntoDb(commonData) == False:
			logger.error('Unable to insert into db. Skipping this 13-F Form ' + commonData['form13FFileNumber']) 
			return	
		logger.debug(pathToFile + 'Written to db succesfully') 


if __name__ == "__main__":	
	initLogger()
	setupDb()
	for year in xrange(2016, 2017):
		for qtr in xrange(1, 5):
			print str(year) + 'Q' + str(qtr) 
			downloadAndProcess13FFromIndex(year, qtr)
	downloadIndexFilesInRange(2016,2017)
	#setupDb()
	#parse13F("./Data_13F/2016Q1_740913.xml")
	#xmlTree = retrieveXMLFile('https://www.sec.gov/Archives/edgar/data/921669/000114036117007268/primary_doc.xml', 'test.xml')
    #downloadXMLFromCompanyIndex(sys.argv[1])