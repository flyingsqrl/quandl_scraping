# -*- coding: utf-8 -*-
"""
Script Name: USDA_LM_PK602_SCRAPER

Purpose:
Scrape daily USDA inspected pork figures from LM_PK602
for upload to Quandl.com.  This script is designed to access the data using USDA's
Livestock Market Report (LMR) web service.  There are various sections to the 
report (cutout value, daily volume, individual cut values.  The script breaks
each out to create seperate Quandl codes.

Approach:
Access URL (LMR web service) and iterate through the resulting XML output.
For each day in the file (file could cover multiple days), extract the relevant
data into a Pandas dataframe, clean/manipulate and then write into CSV format
with appropriate Quandl metadata in the header for upload to Quandl.

Author: Bill Cary

History:

Date            Author      Purpose
-----           -------     -----------
05/12/2014      Bill Cary   Initial development

"""

import urllib2
from dateutil import parser
from bs4 import BeautifulSoup
import pandas as pd
import pandas.tseries.offsets as pto
import StringIO
import datetime

# Set the date range for the report (4 days to ensure all data is captured)
startdate = datetime.datetime.now() - 4 * pto.BDay()

startdate = startdate.strftime('%m/%d/%Y')
print startdate

# Access and open the URL for the report
#target_url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/pork/LM_PK602?filter={"filters":[{"fieldName":"Report Date","operatorType":"GREATER","values":[{0}]}]}'.format(startdate)

target_url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/pork/LM_PK602?filter={"filters":[{"fieldName":"Report Date","operatorType":"GREATER","values":["04/30/2014"]}]}'

print target_url
#fileobj = urllib2.urlopen(target_url)
#
## Initialize a list to hold the entire URL and the relevant lines of data
#url_lines = []
#for line in fileobj:
#    url_lines.append(line)
#    
## Extract week ending date from report (contained on 7th line of file)
#wk_ending = url_lines[6].strip()[12:]
#
#dt = parser.parse(wk_ending)
#
#dt = pd.to_datetime(dt)   
#
## Add relevant lines to a list and clean up        
#data_lines = url_lines[8:16]
#del data_lines[1]
#
#data_lines = [x.strip().split() for x in data_lines]
#
#del data_lines[0][8] # remove the '/n' after 'Bison' by dropping element
#
#headers = data_lines.pop(0) # extract headers in prep for pandas
#
#df = pd.DataFrame(data_lines, columns=headers) # create pandas dataframe
#df.insert(0, 'Wk_Ending_Date', dt.strftime('%Y-%m-%d')) # add week ending date column
#
##offset = df.index - len(df.index)
#
#start = dt - pd.DateOffset(len(df.index)-1)
#
#Slaughter_Date = pd.date_range(start, dt)
#    
#df.insert(0, 'Slaughter_Date', Slaughter_Date)
#
#df.replace('-', 0, inplace=True)  # replace '-' with zeros
#
## Remove commas from numeric columns
#df['Cattle'] = df['Cattle'].str.replace(',', '')
#df['Calves'] = df['Calves'].str.replace(',', '')
#df['Hogs'] = df['Hogs'].str.replace(',', '')
#df['Sheep'] = df['Sheep'].str.replace(',', '')
#df['Goats'] = df['Goats'].str.replace(',', '')
#df['Equine'] = df['Equine'].str.replace(',', '')
#df['Bison'] = df['Bison'].str.replace(',', '')
#
#
## Drop non-numeric columns for quandl compatibility
#df.drop(['Wk_Ending_Date', 'Day'], inplace = True, axis = 1)
#
## Print quandl CSV metadata
#print 'code: USDA_SJ_LS711_WKLY_SLGTR'
#print 'name: USDA SJ_LS711 Weekly Livestock Slaughter by Day'
#print 'description: Daily slaughter for the week, by species, from USDA SJ_LS711 report'
#print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt'
#print 'frequency: daily'
#print 'private: true'
#print '---'
#
## Print contents of dataframe in CSV format
#io = StringIO.StringIO()
#df.to_csv(io, index=False, date_format = '%Y-%m-%d')
#
#print io.getvalue()