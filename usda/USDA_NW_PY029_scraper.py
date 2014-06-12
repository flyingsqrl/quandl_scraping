# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 09:18:58 2014

@author: nataliecmoore

Script Name: USDA_NW_PY029_SCRAPER

Purpose:
Retrieve daily national turkey parts data from the NW_PY029 report via the USDA
for upload to Quandl.com. The script pulls data for volume and weighted average price
for each turkey cut tracked by the USDA.

Approach:
Split the website data into lines starting where the name of the cut began and
ending at the new line character. Then created a table for each cut to hold
the weighted average price and volume formatted for upload to quandl.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/05/2014      Natalie Moore   Initial development/release

"""


import re
import urllib2
import pandas as pd
import sys
import datetime
import pytz

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') # holds the date in YYYY-MM-DD format
# stores report in variable "site_contents"
url='http://www.ams.usda.gov/mnreports/nw_py029.txt'
site_contents=urllib2.urlopen(url).read()

start=site_contents.find('(000)') # index of point in the domestic trading section before names of cuts begin
ending_index=site_contents.find('BREASTS, 4-8 LBS GRADE A', start)-2 # index of breakpoint before the name of the first cut
lines=[] # list to later hold each line of the website data
# Loops through the domestic trading section, getting the line of data from the name
# of the cut to the volume value and splits into a list 
while site_contents.find('EXPORT TRADING')!=ending_index+4:
    starting_index=ending_index+2 # starting_index is 2 after ending_index (because it marks the '\r\n' index)
    ending_index=site_contents.find('\r\n', starting_index+2) # ending_index becomes the beginning index of the next '\r\n'
    line=site_contents[starting_index:ending_index] 
    line=re.compile('\s\s+').split(line) # splits each line and creates a list with each data point
    if len(line)!=2:    
        line[3]=re.compile('\s').split(line[3])[0] # separate line[3] by the space    
    lines.append(line) # add line to list of all the lines
# Loops through each line in lines and finds the weighted average and volume. Then
# creates a table for each cut and formats for upload to quandl.
x=0
while x<len(lines):
    if len(lines[x])==2: # if no data, set weighted average and volume to 0
        weighted_average=0
        volume=0
    else: # if data, find weighted average and volume and convert to floats
        weighted_average=float(lines[x][2])
        volume=float(lines[x][3])
    if  lines[x][0][len(lines[x][0])-1]=='/': # remove the 1/, 2/, 3/ 4/ from end of names if needed
        name=lines[x][0][:(len(lines[x][0])-3)]
    else:
        name=lines[x][0]
    headings=['Date', 'Weighted Average', 'Volume (000)']
    data={'Date': [date], 'Weighted Average': [weighted_average], 'Volume (000)': [volume]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', name) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    quandl_code = 'USDA_NW_PY029_'+name2+'\r'
    print 'code: ' + quandl_code
    print 'name: Daily National Turkey Parts- '+name.title()+'\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'description:  Weighted average price and volume of turkey parts' \
    '\n  from the USDA NW_PY029 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers ' + name.lower() + '.\n'\
    + reference_text 
    print 'reference_url: http://www.ams.usda.gov/mnreports/nw_py029.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    x=x+1
    