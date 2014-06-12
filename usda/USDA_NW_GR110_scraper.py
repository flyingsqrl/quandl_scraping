# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 11:26:32 2014

@author: nataliecmoore

Script Name: USDA_NW_GR110_SCRAPER

Purpose:
Retrieve Iowa daily grain prices from the NW_GR110 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for
minimum bid, maximum bid, and average bid for #2 Yellow Corn and #1 Yellow
Soybeans at each Iowa region.

Approach:
Used string indexing to get data for each location and then used pyparsing
to divide that data into its components.

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
import re

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') # holds the date in YYYY-MM-DD format
# stores report in variable "site_contents"
url='http://www.ams.usda.gov/mnreports/nw_gr110.txt'
site_contents=urllib2.urlopen(url).read()

# list of each location in the report
labels=['Northwest', 'North Central', 'Northeast', 'Southwest', 'South Central', 'Southeast']

# list of each crop in the report
crop_labels=['#2 Yellow Corn', '#1 Yellow Soybeans']

ending_index=0 # initializes to 0 so that it can be later used to divide site_contents into sections

# Loops through each location in labels and finds the data for that location.
# Pyparsing is used to store each data element in a list so that it can be
# used to create a table.
x=0
while x<len(labels):
    starting_index=site_contents.find(labels[x], ending_index)
    line=Suppress(Literal(labels[x]))+(Word(nums+'.')+Suppress(Literal('\x96'))+Word(nums+'.')+Word(nums+'.'))*2
    ending_index=site_contents.find('\r\n', starting_index)
    line=line.parseString(site_contents[starting_index:ending_index])
    # Loops through each crop in crop_labels and creates a table using the location
    # in labels and the crop in crop_labels.     
    y=0
    while y<len(crop_labels):
        headings=[ 'Date', 'Minimum Bid', 'Maximum Bid', 'Average Bid']
        data={'Date': [date], 'Minimum Bid': [line[0]], 'Maximum Bid': [line[1]], 'Average Bid': [line[2]]}
        del line[0:3]
        data_df=pd.DataFrame(data, columns=headings)
        data_df.index=data_df['Date']
        data_df=data_df.drop('Date', 1)
        replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
        remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@ ]') # list of characters to be removed from the pork cut description
        name1 = replace.sub('_', crop_labels[y].upper()) # replace certain characters with '_'
        name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
        name2 = name2.translate(None, '-') # ensure '-' character is removed
        quandl_code='USDA_NW_GR110_'+labels[x].upper().replace(' ', '_')+'_'+name2+"\n"
        reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
        'at http://mpr.datamart.ams.usda.gov.\n' 
        print 'code: ' + quandl_code+'\n'
        print 'name: '+labels[x]+' Iowa Daily Grain Prices- '+crop_labels[y]+'\n'
        print 'description: Daily minimum, maximum, and average bids for '+labels[x]+' Iowa '+crop_labels[y]+ ' from the USDA NW_GR110 report published by the USDA Agricultural Marketing Service ' \
        '(AMS). Prices represent $/bu.' + reference_text
        print 'reference_url: http://www.ams.usda.gov/mnreports/nw_gr110.txt\n'
        print 'frequency: daily\n'
        print 'private: false\n'
        print '---\n'
        data_df.to_csv(sys.stdout)
        print '\n'
        print '\n'
        y=y+1
    x=x+1


