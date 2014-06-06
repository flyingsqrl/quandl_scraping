# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 14:38:08 2014

@author: nataliecmoore

Script Name: USDA_LM_HG210_SCRAPER

Purpose:
Retrieve daily Eastern Cornbelt USDA data from the LM_HG210 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for the base price 
range and the weighted average base price for slaughtered hogs.

Approach:
Used string indexing to find the base price section of the website and then
store the base price information into a list and then a table.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/04/2014      Natalie Moore   Initial development/release

"""

import urllib2
import pytz
import pandas as pd
import datetime 
import sys

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d") # holds the date in YYYY-MM-DD format
# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/lm_hg210.txt"
site_contents=urllib2.urlopen(url).read()

starting_index=site_contents.find("NEGOTIATED PURCHASE") # store the index of the beginning of the section
ending_index=site_contents.find("--", starting_index) # store the index of the end of the section

# if no prices are reported store 0 for each value
if site_contents.find("*Price not reported due to confidentiality*", starting_index, ending_index)!=-1:
    base_price=[0, 0, 0]
# if prices are reported use indexing to find and store the relevant values
else:
    base_price_index=site_contents.rfind("Base Price Range", 0, ending_index) # store the index of the beginning of the base price section
    base_price_start=site_contents.find('$', base_price_index) # store the index of where the base price data value begins
    base_price_end=site_contents.find(',', base_price_start) # store the index of where the base data value ends
    base_price=site_contents[base_price_start+1:base_price_end].split('-') # split the value into min and max value and store in list
    base_price=[float(y.replace('$', '')) for y in base_price] # remove $ and convert values to floats
    weighted_average_index=site_contents.rfind("Weighted Average", 0, ending_index) # store the index of the beginning of the weighted average section
    weighted_average_start=site_contents.find("$", weighted_average_index) # find where the weighted average value begins
    weighted_average_end=site_contents.find('\r\n', weighted_average_start) # find end of weighted average value
    base_price.append(float(site_contents[weighted_average_start+1:weighted_average_end])) # add weighted average to base_price and convert to float
    
headings=[ 'Date', 'Minimum Base Price', 'Maximum Base Price', 'Weighted Average']
data={'Date': [date], 'Minimum Base Price': [base_price[0]], 'Maximum Base Price': [base_price[1]], \
      'Weighted Average': [base_price[2]]}
data_df=pd.DataFrame(data, columns=headings)
data_df.index=data_df['Date']
data_df=data_df.drop('Date', 1)
quandl_code='USDA_LM_HG210_EASTERN_BASE_PRICE\r'
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' 
print 'code: ' + quandl_code+'\n'
print 'name: Eastern Cornbelt Hog Report- Negotiated Purchase Base Price \n'
print 'description: Eastern Cornbelt daily negotiated purchase base price including minimum/maximum base price and weighted average ' \
'from the USDA LM_HG210 report published by the USDA Agricultural Marketing Service ' \
'(AMS).  \n'\
+ reference_text+'\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_hg210.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
data_df.to_csv(sys.stdout)
print '\n'
print '\n'
