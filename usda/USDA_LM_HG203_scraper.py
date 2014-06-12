# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 13:36:23 2014

@author: nataliecmoore

Script Name: USDA_LM_HG203_SCRAPER

Purpose:
Retrieve daily National USDA data from the LM_HG203 report via the USDA LMR
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

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') # holds the date in YYYY-MM-DD format
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/lm_hg203.txt'
site_contents=urllib2.urlopen(url).read()

start=site_contents.find('NEGOTIATED PURCHASE') # store the index of the beginning of the negotiated purchase section
end=site_contents.find('--', start)     # store the index of the end of the negotiated purchase section
base_price_index=site_contents.rfind('Base Price Range', start, end) # store the index of the beginning of the base price section
weighted_average_index=site_contents.rfind('Weighted Average', start, end) #store the index of the weighted average section

base_price_start=site_contents.find('$', base_price_index) # store the index of where the base price values begin
base_price_end=site_contents.find(',', base_price_index)   # store the index of where the base price values end

weighted_average_start=site_contents.find('$', weighted_average_index) # store the index of the beginning of the w.a. value
weighted_average_end=site_contents.find('\r\n', weighted_average_start) # store the index of the end of the w.a. value

base_price=site_contents[base_price_start+1:base_price_end].split('-') # add the base price min and max range to "base_price"
base_price=[float(x.replace('$', '').strip()) for x in base_price] # remove $ and convert value to float
weighted_average=float(site_contents[weighted_average_start+1:weighted_average_end]) # find weighted average and convert to float
base_price.append(weighted_average) # store weighted average in base_price

headings=[ 'Date', 'Minimum Base Price', 'Maximum Base Price', 'Weighted Average']
data={'Date': [date], 'Minimum Base Price': [base_price[0]], 'Maximum Base Price': [base_price[1]], \
      'Weighted Average': [base_price[2]]}
data_df=pd.DataFrame(data, columns=headings)
data_df.index=data_df['Date']
data_df=data_df.drop('Date', 1)
quandl_code='USDA_LM_HG203_NATIONAL_BASE_PRICE\r'
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' 
print 'code: ' + quandl_code+'\n'
print 'name: National Hog Report- Negotiated Purchase Base Price \n'
print 'description: National daily negotiated purchase base price including minimum/maximum base price and weighted average ' \
'from the USDA LM_HG203 report published by the USDA Agricultural Marketing Service ' \
'(AMS).  \n'\
+ reference_text+'\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_hg203.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
data_df.to_csv(sys.stdout)
print '\n'
print '\n'








