#!/usr/bin/python

import sys
import urllib

print 'Number of arguments:', len(sys.argv), 'arguments.'
print 'Argument List:', str(sys.argv)

#init
CIK_list = ['1048445', '1350694']
baseURL = 'https://www.sec.gov/Archives/'
dict = {}

lines = open(sys.argv[1]).read().splitlines()

for line in lines:
	if line.find('13F-HR') != -1:
		for CIK in CIK_list:
			if line.find(CIK) != -1:
				words = line.split()
				dict.update({CIK:words[-1]});

for CIK,URL in dict.items():
	print(CIK, URL)
	urllib.urlretrieve (baseURL + URL, './Data_13F/2017Q1_' + CIK + '.xml')  #time need to be a argument

