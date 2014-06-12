# -*- coding: utf-8 -*-
"""
Created on Thu Jun 12 08:03:11 2014

@author: nataliecmoore

Script Name: USDA_LM_HG230_HIST_SCRAPER

Purpose:
Retrieve the prior day national sow purchase USDA data from the LM_HG230 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for the head count,
average weight, price range, and weighted average price for purchased sows in
the weight ranges: 300-399, 400-449, 450-499, 500-549, and 550+ for the past 4 days.

Approach:
Accessed the data from the LMR web service in XML format. Looped through each XML sub-section
until the relevant data was found. Then looped through each date and created a table for 
each that was formatted for upload to quandl.com.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/12/2014      Natalie Moore   Initial development/release

"""

import pandas as pd
import pandas.tseries.offsets as pto
import datetime
import urllib2
import sys
from lxml import objectify

# Set the date range for the report (4 days to ensure all data is captured)
startdate = datetime.datetime.now() - 4 * pto.BDay()
startdate = startdate.strftime('%m/%d/%Y')

target_url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/hogs/LM_HG230?filter={%22filters%22:[{%22fieldName%22:%22Report%20Date%22,%22operatorType%22:%22GREATER%22,%22values%22:[%22'+startdate+'%22]}]}'

fileobj = urllib2.urlopen(target_url).read()

data=[]

root = objectify.fromstring(fileobj)

for report_date in root.report.iterchildren(): # processing must be repeated for each day covered by the report
    date = report_date.attrib['reported_for_date'] # the prior day is listed under the reported_for_date
    for report in report_date.iterchildren():
        if report.attrib['label']=='Sows':
            for item in report.iterchildren(): # for each item in report's children
                data.append([date, item.attrib['weight_range_desc'], item.attrib['head_count'], item.attrib['avg_weight'], \
                item.attrib['price_low'], item.attrib['price_high'], item.attrib['wtd_avg']]) # add relevant data to list "data"
            
# Loops through each date and adds its data to quandl.     
x=0
while x<len(data):
    headings=['Date', 'Head Count', 'Avg Wgt', 'Low Price', 'High Price', 'Wtd Avg Price']
    hg_data={'Date': [data[x][0]], 'Head Count': [data[x][2]], 'Avg Wgt': [data[x][3]], 'Low Price': [data[x][4]], \
           'High Price': [data[x][5]], 'Wtd Avg Price': [data[x][6]]}
    hg_data_df = pd.DataFrame(hg_data, columns = headings)
    hg_data_df.index = hg_data_df['Date']
    hg_data_df = hg_data_df.drop('Date', 1)
    quandl_code = 'USDA_LM_HG230_'+data[x][1].replace('-', '_').replace('/', '_')+'\r'# build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'code: ' + quandl_code+'\n'
    print 'name: National Daily Sows Purchased- '+data[x][1]+' pounds\n'
    print 'description: National daily direct sow and boar report. This dataset contains '\
    ' head count, average weight, price range, and weighted average for sows in the weight range '+data[x][1]+\
    ' from the USDA LM_HG230 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS).\n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_hg230.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    hg_data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1