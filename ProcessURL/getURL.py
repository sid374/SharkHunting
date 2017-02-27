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

def downloadAndProcess13FFromIndex(year, qtr, CIK_list = ['1048445', '921669', '1040273', '1418814', '1336528', '1365341'], force = False):
	setupDb()
	downloadIndexFile(year, qtr)
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
		#print(CIK, URL)
		fPath = get13FFilePathForYearAndQuarter(year, qtr, CIK)
		if force == False and os.path.isfile(fPath):
			print 'File already exists, use force=True to redownload'
		else:
			try:
				abcabc = urllib.urlretrieve(baseURL + URL, fPath)
			except:
				pdb.set_trace()
		parse13F(fPath)


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
			print 'File already exists, use force=True to redownload'
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
			print 'Downloading ' + str(year) + 'Q' + str(i)
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
		pdb.set_trace()
		print stockInfo
	return stockInfo

def parse13F(pathToFile):
	'''
		Parses the .txt 13-f files that we download from the idx files above
	'''
	print 'Parsing 13F File ' + pathToFile
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
			print pathToFile + 'Could not be parsed. Manual intervention required'
			return
		xmlRootCoverPage = etree.fromstring(xmlCoverPage, parser)

		value = xmlRootCoverPage.find('.//13f:tableValueTotal',namespaces=ns).text
		if int(value) == 0:
			print 'Bogus 13F ' + pathToFile
			return
		commonData = {}
		commonData['submissionType'] = xmlRootCoverPage.find('.//13f:submissionType',namespaces=ns).text
		commonData['periodOfReport'] = datetime.datetime.strptime(xmlRootCoverPage.find('.//13f:periodOfReport',namespaces=ns).text, "%m-%d-%Y")
		commonData['form13FFileNumber'] = xmlRootCoverPage.find('.//13f:form13FFileNumber',namespaces=ns).text
		commonData['cik'] = xmlRootCoverPage.find('.//13f:cik',namespaces=ns).text
		commonData['fundName'] = xmlRootCoverPage.find('.//13f:filingManager/13f:name',namespaces=ns).text

		mstart = re.search('<.*informationTable', doc)
		mend = re.search('</.*informationTable', doc)
		xmlStockTable = doc[mstart.start(0):mend.end(0)]
		xmlRootStockTable = etree.fromstring(xmlStockTable, parser)
		stockList = xmlRootStockTable.findall('.//infoTable:infoTable', namespaces=ns)
		for stock in stockList:
			stockDocument = parseStockXML(stock, ns)
			stockDocument.update(commonData)
			if insertStockIntoDb(stockDocument) == False:
				print 'Unable to insert into db. Skipping this 13-F Form ' + commonData['form13FFileNumber']
				return
		print pathToFile + 'Written to db succesfully'


if __name__ == "__main__":
	for year in xrange(2016, 2017):
		for qtr in xrange(1, 5):
			print str(year) + 'Q' + str(qtr) 
			downloadAndProcess13FFromIndex(year, qtr)
	#downloadIndexFilesInRange(2016,2017)
	#setupDb()
	#parse13F("./Data_13F/2016Q1_1635999.xml")
	#xmlTree = retrieveXMLFile('https://www.sec.gov/Archives/edgar/data/921669/000114036117007268/primary_doc.xml', 'test.xml')
    #downloadXMLFromCompanyIndex(sys.argv[1])