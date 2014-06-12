# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 15:51:49 2014

@author: nataliecmoore
Script Name: USDA_NW_PY021_SCRAPER

Purpose:
Retrieve weekly national turkey slaughter data from the NW_PY021 report via the USDA
for upload to Quandl.com. The script pulls data for head and average live weight
for each region tracked by USDA.

Approach:
Used pyparsing to find the head and average live weight for each region. Then
formatted the data to make a table for each region.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/05/2014      Natalie Moore   Initial development/release

"""
from pyparsing import *
import re
import urllib2
import pandas as pd
import sys
import datetime
import pytz
import calendar

# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/nw_py021.txt'
site_contents=urllib2.urlopen(url).read()

begin_index=site_contents.find('  ', site_contents.find('Week ending')) # beginning index of the date
end_index=site_contents.find('\r\n', begin_index) # ending index of the date
hyphen=site_contents.find('-', begin_index) # find index of the hyphen separating day and month
day=site_contents[begin_index:hyphen].strip() # store the day 
next_hyphen=site_contents.find('-', hyphen+1) # find index of hyphen separating month and year
month=site_contents[hyphen+1:next_hyphen] # store month
year=site_contents[next_hyphen+1:end_index] # store year in YYYY format
date=datetime.date(int(year), list(calendar.month_name).index(month), int(day)).strftime("%Y-%m-%d") # store date in YYYY-mm-dd format

# list of each region in the report
labels=['North East', 'South Atlantic', 'North Central', 'South Central', 'West', 'U.S. total']

# Loops through each region and uses pyparsing to find the head and average 
# live weight for the turkeys slaughtered. 
x=0
while x<len(labels):
    suppress=Suppress(Word(printables))
    line=Literal(labels[x])+suppress*4+Word(nums+',')+Word(nums+'.') # grammar for each line of data following a region
    first=site_contents.find(labels[x]) # index of label
    end=site_contents.find('\r\n', first) # index of end of the line
    line=line.parseString(site_contents[first:end]) # parse line and store in list "line"
    line=[float(y.replace(',','')) for y in line[1:]] # remove commas and convert to floats
    headings=['Date','Actual Turkey Slaughter', 'Turkey Average Weight']
    data={'Date':[date], 'Actual Turkey Slaughter': [line[0]], 'Turkey Average Weight': [line[1]]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    name=labels[x].replace(' ','_').replace('.','')
    quandl_code = 'USDA_NW_PY021_'+name.upper()+'\r'
    print 'code: ' + quandl_code
    print 'name: Weekly National Turkey Slaughter- '+labels[x].title()+'\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'description:  Weekly national turkey slaughter data' \
    '\n  from the USDA NW_PY021 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers the ' + labels[x]+ '.\n'\
    + reference_text 
    print 'reference_url: http://www.ams.usda.gov/mnreports/nw_py021.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    x=x+1
