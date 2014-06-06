# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 10:13:02 2014

@author: nataliecmoore

Script Name: USDA_GX_GR110_SCRAPER

Purpose:
Retrieve daily USDA data from the GX_GR110 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for
the minimum and maximum bids for the past 15 days and past 15-30 days for
soybeans, corn, and srw wheat.

Approach:
Used python string parsing to extract the minimum and maximum bids and
then format in a table for upload to Quandl.com


Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/03/2014      Natalie Moore   Initial development/release

"""


import urllib2
import pytz
import pandas as pd
import datetime 
import sys
import re

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d") # holds the date in YYYY-MM-DD format
# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/gx_gr110.txt"
site_contents=urllib2.urlopen(url).read()

# Stores the names of the crops for the past 15 days in labels_15 and the names
# of the crops formatted to be used in the name of the quandl file in labels_15_names
labels_15=["Soybeans", "Corn", "Corn"]
labels_15_names=["Soybeans", "Corn (Terminal Elevator)", "Corn (Processor)"]

# Stores the names of the crops for the past 15-30 days in labels_30 and the names
# of the crops formatted to be used in the name of the quandl file in labels_30_names
labels_30=["SRW Wheat", "Soybeans", "Corn", "Corn"]
labels_30_names=["SRW Wheat", "Soybeans", "Corn (Terminal Elevator)", "Corn (Processor)"]

# This function takes in an index of a hyphen and returns the minimum and maximum bids
# Precondition: index is a valid index for a hyphen that separates two numerical values
def min_and_max(index):
    space_before=site_contents.rfind(' ', 0, hyphen)
    space_after=site_contents.find(' ', hyphen)
    minimum_bid=site_contents[space_before:hyphen].strip()
    maximum_bid=site_contents[hyphen+1:space_after].strip()
    return [minimum_bid, maximum_bid]

ending_index=0 # used in the following loop for indexing, initialized to 0

# Loops through each crop in labels_15. Finds the minimum and maximum bids
# and formats for upload to quandl.
x=0
while x<len(labels_15):
    ending_index=site_contents.find("Spot", ending_index+1) # bids occur before the word "Spot"
    starting_index=site_contents.rfind(labels_15[x], 0, ending_index) # index of the crop name
    hyphen=site_contents.find('-', starting_index) # index of the hyphen that separates the bids
    bids=min_and_max(hyphen) # calls min_and_max and stores the minimum and maximum bids in list "bids"
    bids=[float(y) for y in bids] # changes bid values to floats
    headings=[ 'Date', 'Minimum Bid', 'Maximum Bid']
    data={'Date': [date], 'Minimum Bid': [bids[0]], 'Maximum Bid': [bids[1]]}
    data_df=pd.DataFrame(data, columns=headings)
    data_df.index=data_df['Date']
    data_df=data_df.drop('Date', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', labels_15_names[x].upper()) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    quandl_code='USDA_GX_GR110_'+name2+'_SPOT\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'code: ' + quandl_code+'\n'
    print 'name: Chicago Terminal Grain Report- Minimum and Maximum Bids (past 15 days)- '+labels_15_names[x]+'\n'
    print 'description: Minimum and maximum bids up to the past 15 days for '+labels_15_names[x]+ \
    ' from the USDA GX_GR110 report published by the USDA Agricultural Marketing Service ' \
    '(AMS). Prices represent $/bu. \n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/gx_gr110.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1    

ending_index=0  # initializes ending_index to 0

# Loops through each crop in labels_30. Finds the minimum and maximum bids
# and formats for upload to quandl.
x=0
while x<len(labels_30):
    ending_index=site_contents.find("30 Days", ending_index+1) # bids occur before the string "30 Days"
    starting_index=site_contents.rfind(labels_30[x],0, ending_index) # index of the crop name
    hyphen=site_contents.find('-', starting_index) # index of the hyphen that seperates the bid values
    bids=min_and_max(hyphen) 
    bids=[float(y) for y in bids]
    headings=[ 'Date', 'Minimum Bid', 'Maximum Bid']
    data={'Date': [date], 'Minimum Bid': [bids[0]], 'Maximum Bid': [bids[1]]}
    data_df=pd.DataFrame(data, columns=headings)
    data_df.index=data_df['Date']
    data_df=data_df.drop('Date', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', labels_30_names[x].upper()) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    quandl_code='USDA_GX_GR110_'+name2+'_30_DAY\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'code: ' + quandl_code+'\n'
    print 'name: Chicago Terminal Grain Report- Minimum and Maximum Bids (Past 15-30 days)- '+labels_30_names[x]+'\n'
    print 'description: Minimum and maximum bids for the past 15-30 days for '+labels_30_names[x]+ \
    ' from the USDA GX_GR110 report published by the USDA Agricultural Marketing Service ' \
    '(AMS). Prices represent $/bu. \n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/gx_gr110.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1    

    
    

