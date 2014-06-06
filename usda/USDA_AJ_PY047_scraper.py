# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 09:57:46 2014

@author: nataliecmoore

Script Name: USDA_AJ_PY047_SCRAPER

Purpose:
Retrieve daily northeast broiler/fryer data from the AJ_PY047 report via the USDA
for upload to Quandl.com. The script pulls data for weighted average price
and volume for each individual cut tracked by USDA.

Approach:
Used pyparsing to find the weighted average and volume for each cut in the report.
Then formatted the data into a table to be uploaded to Quandl.com.

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

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d") # holds the date in YYYY-MM-DD format
# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/aj_py047.txt"
site_contents=urllib2.urlopen(url).read()

# names of each cut in the report
labels=["BREAST - B/S", "TENDERLOINS", "BREAST - WITH RIBS", "BREAST - LINE RUN", "LEGS", "LEG QUARTERS (BULK)",\
        "DRUMSTICKS", "THIGHS", "B/S THIGHS", "WINGS (WHOLE)", "BACKS AND NECKS (STRIPPED)", "LIVERS (5 POUND TUBS)",\
        "GIZZARDS (HEARTS)"]

ending_index=0 # initializes ending_index to 0 to be used in following loop       
# Loops through each cut in labels and uses pyparsing to find the weighted average
# and volume for that cut. The data and data are formatted into a table and the 
# relevant quandl data is printed.
x=0
while x<len(labels):
    line=Literal(labels[x])+Word(nums+'-')+Word(nums+'.')+Word(nums+',') # grammar to find each label's data
    starting_index=site_contents.find(labels[x], ending_index) # stores the index of the beginning of each label's data
    ending_index=site_contents.find('\r\n', starting_index) # stores the index of the end of the label's data
    text=site_contents[starting_index:ending_index] # the line to be parsed is from starting_index to ending_index
    parsed=line.parseString(text) # parses the line and stores it in "parsed"
    headings=['Date', 'Weighted Average (Price)', 'Volume (Lbs)']
    data={'Date': [date], 'Weighted Average (Price)': [parsed[2]], 'Volume (Lbs)': [parsed[3].replace(',','')]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', labels[x]) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    quandl_code = 'USDA_AJ_PY047_'+name2+'\r'
    print 'code: ' + quandl_code
    print 'name: Daily Northeast Broiler/Fryer Parts- ' +labels[x].title()+'\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'description: Daily weighted average price and volume of northeast broiler/fryer parts ' \
    '\n  from the USDA AJ_PY047 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers ' + labels[x].lower() + '.\n'\
    + reference_text
    print 'reference_url: http://www.ams.usda.gov/mnreports/aj_py047.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    x=x+1