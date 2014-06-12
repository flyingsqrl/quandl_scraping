# -*- coding: utf-8 -*-
"""
Created on Fri May 30 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_LM_XB403_SCRAPER

Purpose:
Retrieve the current cutout values of choice and select grade beef as well as
the weight, price range, and weighted average for fresh 50% lean trimmings from
the national daily boxed beef cutout report (USDA LM_XB403)


Approach:
Used pyparsing to find the necessary values and then converted into a data table
that was formatted for upload to quandl.com.


Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/02/2014      Natalie Moore   Initial development/release

"""


from pyparsing import *
import urllib2
import pytz
import pandas as pd
import datetime
import sys

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') #holds the date in YYYY-MM-DD format
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/lm_xb403.txt'
site_contents=urllib2.urlopen(url).read()

name=Literal('Current Cutout Values:')
line=name+Word(nums+'.')+Word(nums+'.') # grammar for current cutout values
starting_index=site_contents.find('Current Cutout Values:')
ending_index=site_contents.find('\r\n', starting_index)
ccv=line.parseString(site_contents[starting_index:ending_index]) # parse line into its data components

current_choice_value=ccv[1] # store choice value
current_select_value=ccv[2] # store select value

name=Literal('Fresh 50% lean trimmings')
line=name+Word(nums)+Word(nums+','+'.')*4 # grammar for lean trimmings values
starting_index=site_contents.find('Fresh 50% lean trimmings') # start index of line of data
ending_index=site_contents.find('\r\n', starting_index) # end index of line of data
beef_trimmings=line.parseString(site_contents[starting_index:ending_index]) # parse line of data
weight=float(beef_trimmings[2].replace(',', '')) # remove commas and convert weight to float
min_price=float(beef_trimmings[3]) # store minimum price
max_price=float(beef_trimmings[4]) # sore maximum price
weighted_average=float(beef_trimmings[5]) # store weighted average

# quandl data for current cutout values
ccv_headings = ['Date', 'Choice Value', 'Select Value']
ccv_data={'Date': [date], 'Choice Value': [current_choice_value], 'Select Value': [current_select_value]}
ccv_df = pd.DataFrame(ccv_data, columns = ccv_headings)
ccv_df.index = ccv_df['Date']
ccv_df.drop(['Date'],inplace=True,axis=1) 


reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'
    
print 'code: USDA_LM_XB403_CURRENT_CUTOUT_VALUES\n'
print 'name: Daily Boxed Beef Cutout Values\n'
print 'description: "Daily boxed beef cutout values from the USDA LM_XB403\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text + '"\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_xb403.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
ccv_df.to_csv(sys.stdout)
print '\n'
print '\n'

# quandl data for fresh 50% lean trimmings
bt_headings = ['Date', 'Weight', '$ Min', '$ Max', 'Weighted Average']
bt_data={'Date': [date], 'Weight': [weight], '$ Min': [min_price], '$ Max': [max_price], 'Weighted Average': [weighted_average]}
bt_df = pd.DataFrame(bt_data, columns = bt_headings)
bt_df.index = bt_df['Date']
bt_df.drop(['Date'],inplace=True,axis=1) 

print 'code: USDA_LM_XB403_FRESH_50_LEAN_BEEF_TRIMMINGS\n'
print 'name: Fresh 50% Lean Beef Trimmings\n'
print 'description: "Daily values for fresh 50% lean beef trimmings from the USDA LM_XB403\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text + '"\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_xb403.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
bt_df.to_csv(sys.stdout)
print '\n'
print '\n'

















