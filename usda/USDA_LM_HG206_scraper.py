# -*- coding: utf-8 -*-
"""
Created on Fri Jun 06 12:57:37 2014

@author: nataliecmoore

Script Name: USDA_LM_HG206_SCRAPER

Purpose:
Retrieve daily Iowa/Minnesota hog data from the LM_HG206 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for head count,
price range, and weighted average price.

Approach:
Used python string parsing to find each piece of data on the website.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/06/2014      Natalie Moore   Initial development/release

"""

from pyparsing import *
import urllib2
import pytz
import pandas as pd
import datetime
import sys

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d") #holds the date in YYYY-MM-DD format
# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/lm_hg206.txt"
site_contents=urllib2.urlopen(url).read()

starting_index=site_contents.find(':', site_contents.find("Barrows & Gilts")) # stores index of colon before head count
end_head=site_contents.find('\r\n', starting_index) # stores index of the end of the head count
head=float(site_contents[starting_index+1:end_head].strip().replace(',','')) # store the head count, replace commas, and convert to a float
price_start=site_contents.find('$', site_contents.find("Base Price Range")) # store index of beginning of price range
hyphen=site_contents.find('-', price_start) # store index of hyphen separating min and max price
low_price=float(site_contents[price_start+1:hyphen].strip()) # store min price
price_end=site_contents.find(',', hyphen) # store index of comma that marks end of price range
high_price=float(site_contents[hyphen+1:price_end].strip().replace('$','')) # store high price and remove $
weighted_average_begin=site_contents.find('$', price_end) # find index of $ before weighted average
weighted_average_end=site_contents.find('\r\n', weighted_average_begin) # find index of line break after weighted average
weighted_average=float(site_contents[weighted_average_begin+1:weighted_average_end].strip()) # store weighted average
headings=['Date', 'Head Count', 'Low Price', 'High Price', 'Weighted Average Price']
data={'Date': [date], 'Head Count': [head], 'Low Price': [low_price], 'High Price': [high_price], \
      'Weighted Average Price': [weighted_average]}
data_df = pd.DataFrame(data, columns = headings)
data_df.index = data_df['Date']
data_df = data_df.drop('Date', 1)
quandl_code = 'USDA_LM_HG206'  # build unique quandl code
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' \
'  All pricing is on a per CWT (100 lbs) basis.'
print 'code: ' + quandl_code
print 'name: Iowa/Minnesota Daily Direct Hog Report' 
print 'description: Daily head count, price range and weighted average price' \
'\n  from the USDA LM_HG206 report published by the USDA Agricultural Marketing Service ' \
'\n  (AMS).'+ reference_text
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_hg206.txt'
print 'frequency: daily'
print 'private: false'
print '---'
data_df.to_csv(sys.stdout)
print ''
print ''