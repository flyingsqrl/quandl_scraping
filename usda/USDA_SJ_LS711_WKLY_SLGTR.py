# -*- coding: utf-8 -*-
"""
Script Name: USDA_SJ_LS711_Scraper

Purpose:
Scrape weekly USDA inspected livestock slaughter totals from SJ_LS711
for upload to Quandl.com.  This script is designed to access the report on the
USDA website and harvest the data from the "Federally Inspected Slaughter By
Species and Day - U.S." section of the report.

Approach:
Access URL (a .txt file) and iterate through each line, reading each line into
a python list to simplify data manipulation.  Identify the relevant lines of
data, add them to a 2nd python list and then create a pandas dataframe from
the list of relevant data.  From the dataframe, write the data to the screen
in csv format for cunsumption by quandl.com.

Author: Bill Cary

History:

Date            Author      Purpose
-----           -------     -----------
05/09/2014      Bill Cary   Initial development

"""

import urllib2
from dateutil import parser
#from datetime import datetime
import pandas as pd
import StringIO

# Access and open the URL for the report
target_url = 'http://www.ams.usda.gov/mnreports/sj_ls711.txt'
fileobj = urllib2.urlopen(target_url)


# Initialize a list to hold the entire URL and the relevant lines of data
url_lines = []
for line in fileobj:
    url_lines.append(line)
    
# Extract week ending date from report (contained on 7th line of file)
wk_ending = url_lines[6].strip()[12:]

dt = parser.parse(wk_ending)

dt = pd.to_datetime(dt)   

# Add relevant lines to a list and clean up        
data_lines = url_lines[8:16]
del data_lines[1]

data_lines = [x.strip().split() for x in data_lines]

del data_lines[0][8] # remove the '/n' after 'Bison' by dropping element

headers = data_lines.pop(0) # extract headers in prep for pandas

df = pd.DataFrame(data_lines, columns=headers) # create pandas dataframe
df.insert(0, 'Wk_Ending_Date', dt.strftime('%Y-%m-%d')) # add week ending date column

#offset = df.index - len(df.index)

start = dt - pd.DateOffset(len(df.index)-1)

Slaughter_Date = pd.date_range(start, dt)
    
df.insert(0, 'Slaughter_Date', Slaughter_Date)

df.replace('-', 0, inplace=True)  # replace '-' with zeros

# Remove commas from numeric columns
df['Cattle'] = df['Cattle'].str.replace(',', '')
df['Calves'] = df['Calves'].str.replace(',', '')
df['Hogs'] = df['Hogs'].str.replace(',', '')
df['Sheep'] = df['Sheep'].str.replace(',', '')
df['Goats'] = df['Goats'].str.replace(',', '')
df['Equine'] = df['Equine'].str.replace(',', '')
df['Bison'] = df['Bison'].str.replace(',', '')


# Drop non-numeric columns for quandl compatibility
df.drop(['Wk_Ending_Date', 'Day'], inplace = True, axis = 1)

# Print quandl CSV metadata
print 'code: USDA_SJ_LS711_WKLY_SLGTR'
print 'name: USDA SJ_LS711 Weekly Livestock Slaughter by Day'
print 'description: Daily slaughter for the week, by species, from USDA SJ_LS711 report'
print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt'
print 'frequency: daily'
print 'private: true'
print '---'

# Print contents of dataframe in CSV format
io = StringIO.StringIO()
df.to_csv(io, index=False, date_format = '%Y-%m-%d')

print io.getvalue()