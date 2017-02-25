#!/usr/bin/python
import requests
from lxml import etree, objectify
import sys
import urllib
import pdb
import datetime

def downloadXMLFromCompanyIndex(pathToIndexFile, CIK_list = ['1048445', '1350694']):
	print 'Index file path:', pathToIndexFile
	baseURL = 'https://www.sec.gov/Archives/'
	dict = {}
	lines = open(pathToIndexFile).read().splitlines()

	for line in lines:
		if line.find('13F-HR') != -1:
			for CIK in CIK_list:
				if line.find(CIK) != -1:
					words = line.split()
					dict.update({CIK:words[-1]});

	for CIK,URL in dict.items():
		print(CIK, URL)
		urllib.urlretrieve (baseURL + URL, './Data_13F/2017Q1_' + CIK + '.xml')  #time need to be a argument


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


def downloadIndexFile(year =  2017, qtr = 1, fileType = "company.idx"):
	'''
		fileType: one of the following -> company.idx, master.idx, form.idx  [read https://www.sec.gov/edgar/indices/fullindex.htm for more info]
		Description: given a year and qtr, downlads the file type specified from the edgar database
	'''
	baseURL = 'https://www.sec.gov/Archives/edgar/full-index/'
	fileContents = retrieveURL(baseURL + '/' + str(year) + '/QTR' + str(qtr) + '/' + fileType )
	downloadPath = './Index/' + str(year) + 'Q' + str(qtr) + fileType
	with open(downloadPath, 'w') as outFile:
		outFile.write(fileContents)

def stripNamespaceFromTag(elem):
	'''
		Helper function: given an etree element strips the namespace from the tag for easier reading
	'''
	if '}' in elem.tag:
			elem.tag = elem.tag.split('}', 1)[1]  

def parseStockXML(xmlStockRoot, ns):
	'''	
		Helper function: Given a etree element and namespaces dict with the stock infotable as root, this function parses the stock
	'''
	stockInfo = {}
	for i in xmlStockRoot:
		stripNamespaceFromTag(i)
		if i.tag == 'shrsOrPrnAmt':
			stripNamespaceFromTag(i[0])
			stripNamespaceFromTag(i[1])
			stockInfo[i[0].tag.strip()] = i[0].text.strip()
			stockInfo[i[1].tag.strip()] = i[1].text.strip()
		else:
			stockInfo[i.tag.strip()] = i.text.strip()
	return stockInfo

def parse13F(pathToFile):
	'''
		Parses the .txt 13-f files that we download from the idx files above
	'''
	ns = {	
			'x':'http://www.sec.gov/edgar/thirteenffiler',
			'infoTable':'http://www.sec.gov/edgar/document/thirteenf/informationtable'
		 }
	parser = etree.XMLParser(recover=True) #recovers from broken xml files
	with open(pathToFile, 'r') as file:
		doc = file.read()
		xmlCoverPage = doc[doc.find("<edgarSubmission"):doc.find("</edgarSubmission ")]
		xmlRootCoverPage = etree.fromstring(xmlCoverPage, parser)

		commonData = {}
		commonData['submissionType'] = xmlRootCoverPage.find('.//x:submissionType',namespaces=ns).text
		commonData['periodOfReport'] = datetime.datetime.strptime(xmlRootCoverPage.find('.//x:periodOfReport',namespaces=ns).text, "%m-%d-%Y").date()
		commonData['form13FFileNumber'] = xmlRootCoverPage.find('.//x:form13FFileNumber',namespaces=ns).text
		commonData['cik'] = xmlRootCoverPage.find('.//x:cik',namespaces=ns).text
		commonData['fundName'] = xmlRootCoverPage.find('.//x:filingManager/x:name',namespaces=ns).text

		xmlStockTable = doc[doc.find("<informationTable"):doc.find("</informationTable")]
		xmlRootStockTable = etree.fromstring(xmlStockTable, parser)
		stockList = xmlRootStockTable.findall('.//infoTable:infoTable', namespaces=ns)
		for stock in stockList:
			stockInfo = parseStockXML(stock, ns)
			stockInfo.update(commonData)
			print stockInfo
			#we need to store this dictionary into a datbase next


if __name__ == "__main__":
	parse13F("./Data_13F/2017Q1_1048445.xml")
	# for year in xrange(1993,2017):
	# 	for i in xrange(1,5):
	# 		downloadIndexFile(year, i)

	#xmlTree = retrieveXMLFile('https://www.sec.gov/Archives/edgar/data/921669/000114036117007268/primary_doc.xml', 'test.xml')
    #downloadXMLFromCompanyIndex(sys.argv[1])