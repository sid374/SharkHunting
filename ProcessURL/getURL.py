#!/usr/bin/python
import requests
from lxml import etree
import sys
import urllib

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



if __name__ == "__main__":
	downloadIndexFile(2017, 1)
	#xmlTree = retrieveXMLFile('https://www.sec.gov/Archives/edgar/data/921669/000114036117007268/primary_doc.xml', 'test.xml')
    #downloadXMLFromCompanyIndex(sys.argv[1])