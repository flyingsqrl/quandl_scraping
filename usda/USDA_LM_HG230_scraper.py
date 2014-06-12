# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 15:57:58 2014

@author: nataliecmoore

Script Name: USDA_LM_HG230_SCRAPER

Purpose:
Retrieve the prior day national sow purchase USDA data from the LM_HG230 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for the head count,
average weight, price range, and weighted average price for purchased sows in
the weight ranges: 300-399, 400-449, 450-499, 500-549, and 550+.

Approach:
Used string indexing to find sow purchase section of data. Then used pyparsing
to divide data into lines and store each data element into a table formatted
to be uploaded.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/04/2014      Natalie Moore   Initial development/release

"""

from pyparsing import *
import urllib2
import pytz
import pandas as pd
import datetime 
import sys

date=datetime.datetime.now(pytz.timezone('US/Eastern')) 
date=(date-datetime.timedelta(hours=24)).strftime('%Y-%m-%d')
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/lm_hg230.txt'
site_contents=urllib2.urlopen(url).read()

start_index=site_contents.find('Sows Purchased (Live and Carcass Basis)')
labels=['300-399', '400-449', '450-499', '500-549', '550/up']
x=0
parsed=[]
# Loops through each label in labels and parses its line of data on the website.
# Then it creates a table with the parsed data elements and moves to the next label.
while x<len(labels):
    label_index=site_contents.find(labels[x], start_index) # index of labels[x] on the website
    #grammar for each line of data    
    line_grammar=Literal(labels[x])+Word(nums+',')+Word(nums)+Word(nums+'.'+'-')+Word(nums+'.')
    line_end=site_contents.find('\r\n', label_index) # index of the end of the line to be parsed
    parsed=line_grammar.parseString(site_contents[label_index:line_end]).asList() # parses line and converts to list
    parsed.append(parsed[4]) # add the weighted average to end of the list because split on next line will overwrite parsed[4]
    [parsed[3], parsed[4]]=parsed[3].split('-') # split the price range into low price and high price
    headings=['Date', 'Head Count', 'Avg Wgt', 'Low Price', 'High Price', 'Wtd Avg Price']
    data={'Date': [date], 'Head Count': [parsed[1]], 'Avg Wgt': [parsed[2]], 'Low Price': [parsed[3]], \
           'High Price': [parsed[4]], 'Wtd Avg Price': [parsed[5]]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    quandl_code = 'USDA_LM_HG230_'+parsed[0].replace('-', '_').replace('/', '_')+'\r'# build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'code: ' + quandl_code+'\n'
    print 'name: National Daily Sows Purchased- '+parsed[0]+' pounds\n'
    print 'description: National daily direct sow and boar report. This dataset contains '\
    ' head count, average weight, price range, and weighted average for sows in the weight range '+parsed[0]+\
    ' from the USDA LM_HG230 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS).\n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_hg230.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1

