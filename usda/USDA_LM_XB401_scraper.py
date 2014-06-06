# -*- coding: utf-8 -*-
"""
Created on Fri May 30 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_LM_XB401_SCRAPER

Purpose:
Retrieve daily USDA beef data from the LM_XB401 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for
weight, min/max price, and weighted average for central beef (fresh 90%, frozen 90%, and fresh 85%)


Approach:
Used pyparsing to find each line of data and store each piece of data into its 
corresponding list.


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
import re
import datetime
import sys

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d") #holds the date in YYYY-MM-DD format

# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/lm_xb401.txt"
site_contents=urllib2.urlopen(url).read()

names=[]                                                   # holds name of each beef grade
weight=[]                                                  # holds weight of each beef grade
min_price=[]                                               # holds minimum price
max_price=[]                                               # holds maximum price
weighted_average=[]                                        # holds weighted_average
lines=[]                                                   # holds each relevant line of data
starting_index=site_contents.find("Fresh  90%")            # starting index of relevant website section
ending_index=site_contents.find("Frozen 85%")              # ending index of website section
break_point=site_contents.find("\r\n", starting_index)     # ending index of first line
break_point2=site_contents.find("\r\n", break_point+1)     # ending index of second line
lines.append(site_contents[starting_index:break_point])    # store website data from starting point to end of first line
lines.append(site_contents[break_point+4:break_point2])    # store website data from end of first line (after 4 char new line string) to end of second line
lines.append(site_contents[break_point2+4:ending_index-4]) # store website data from end of second line (after new line) to ending index (before new line)


# Grammar for each line of the website
nonempty_line=Word(alphas)+Word(printables)+Word(nums)+Word(printables)+Word(printables)+Word(printables)+Word(printables)
empty_line=Word(alphas)+Word(printables)
line=nonempty_line | empty_line
parsed=[]                                                  # holds parsed lines


# parses each line and adds to parsed list
x=0
while x<len(lines):
    parsed.append(line.parseString(lines[x]))
    x=x+1

# stores each piece of data into its respective list    
x=0
while x<len(parsed):
    if len(parsed[x])!=2:
        names.append(" ".join(parsed[x][0:2]))                 # add first two strings in parsed list to names list
        weight.append(float(parsed[x][3].replace(",", "")))    # store weight in weight list, remove commas and convert to float
        min_price.append(float(parsed[x][4].replace("$", ""))) # store the minimum price in min_price, remove $ and convert to float
        max_price.append(float(parsed[x][5].replace("$", ""))) # store the maximum price in max_price, remove $ and convert to float
        weighted_average.append(float(parsed[x][6].replace("$", ""))) # store weighted average into weighted_average, remove $ and convert to float
    else:
        names.append(" ".join(parsed[x][0:]))
        weight.append(0)
        min_price.append(0)
        max_price.append(0)
        weighted_average.append(0)
    x=x+1

# This loops through each piece of data in lines and makes a table in csv format and formats for
# upload to Quandl.        
x=0
while x<len(lines):
    headings = ['Date', 'Weight', '$ Min', '$ Max', 'Weighted Average']
    data={'Date': [date], 'Weight': [weight[x]], '$ Min': [min_price[x]], '$ Max': [max_price[x]], 'Weighted Average': [weighted_average[x]]}
    cl_df = pd.DataFrame(data, columns = headings)
    cl_df.index = cl_df['Date']
    cl_df.drop(['Date'],inplace=True,axis=1) 
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', names[x]) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'
    print 'code: USDA_LM_XB401_'+name2
    print 'name: Central Beef'+' - '+names[x]+' '+'\n'
    print 'description: "Daily values for central beef from the USDA LM_XB401\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text + '"\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_xb401.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    cl_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1


