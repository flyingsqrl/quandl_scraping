# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 11:13:54 2014

@author: nataliecmoore

Script Name: USDA_NW_PY002_SCRAPER

Purpose:
Retrieve weekly national broiler/fryer data from the NW_PY002 report via the USDA
for upload to Quandl.com. The script pulls data for head and average live weight
for each weight category tracked by USDA.

Approach:
Used indexing to add the head and average live weight data to a list. Then
iterated over each weight category to make a table using the previously found
data and formmated for upload to quandl.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/05/2014      Natalie Moore   Initial development/release

"""

from pyparsing import *
import urllib2
import pytz
import pandas as pd
import datetime 
import sys
import re
import calendar

# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/nw_py002.txt"
site_contents=urllib2.urlopen(url).read()

begin_index=site_contents.find('  ', site_contents.find("Week ending")) # beginning index of the date
end_index=site_contents.find('\r\n', begin_index) # ending index of the date
hyphen=site_contents.find('-', begin_index) # find index of the hyphen separating day and month
day=site_contents[begin_index:hyphen].strip() # store the day 
next_hyphen=site_contents.find('-', hyphen+1) # find index of hyphen separating month and year
month=site_contents[hyphen+1:next_hyphen] # store month
year='20'+site_contents[next_hyphen+1:end_index] # store year and append '20' so it's in YYYY format
date=datetime.date(int(year), list(calendar.month_name).index(month), int(day)).strftime("%Y-%m-%d") # store date in YYYY-mm-dd format

labels=["Head", "Avg Live Wgt"]
weight_labels=["4.25 lbs & down", "4.26-6.25 lbs", "6.26-7.75 lbs", "7.76 lbs & up", "Total"]
lines=[]
# Loops through each label and adds the data for each in "lines"
x=0
while x<len(labels):
    start=site_contents.find(labels[x])
    end=site_contents.find('\r\n', start)
    line=site_contents[start:end].split("  ")
    line=[y for y in line[1:] if len(y)!=0]
    line=[float(y.replace(',','')) for y in line]
    lines.append(line)
    x=x+1
# Loops through each weight label and creates a table with the head and average live weight
# values for each.
y=0
while y<len(weight_labels):
    headings=["Date", "Head", "Average Live Weight"]
    data={"Date": [date], "Head": [lines[0][y]], "Average Live Weight": [lines[1][y]]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    replace = re.compile('[ /-]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', weight_labels[y]) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    quandl_code = 'USDA_NW_PY002_'+name2+'\r'
    print 'code: ' + quandl_code
    print 'name: Weekly National Chicken Slaughter- ' +weight_labels[y].title()+'\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'description: Weekly head and average live weight of broiler/fryer chickens ' \
    '\n  from the USDA NW_PY002 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers ' + weight_labels[y].lower() + '.\n'\
    + reference_text    
    print 'reference_url: http://www.ams.usda.gov/mnreports/nw_py002.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    y=y+1

    
    
    
    
    
    
    