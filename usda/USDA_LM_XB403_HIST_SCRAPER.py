# -*- coding: utf-8 -*-
"""
Created on Thu Jun 12 11:07:02 2014

@author: nataliecmoore

Script Name: USDA_LM_XB403_HIST_SCRAPER

Purpose:
Retrieve the current cutout values of choice and select grade beef as well as
the weight, price range, and weighted average for fresh 50% lean trimmings 
for the past four days from the national daily boxed beef cutout report (USDA LM_XB403)

Approach:
Accessed the report in an XML format. Iterated through each section and subsection
of the report looking for the relevant data and then created a data table for each of 
the past four days.

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

target_url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/beef/LM_XB403?filter={%22filters%22:[{%22fieldName%22:%22Report%20Date%22,%22operatorType%22:%22GREATER%22,%22values%22:[%22'+startdate+'%22]}]}'

fileobj = urllib2.urlopen(target_url).read()

data=[]
root = objectify.fromstring(fileobj)

for report_date in root.report.iterchildren(): #processing must be repeated for each day covered by the report
    date = report_date.attrib['report_date']
    for report in report_date.iterchildren(): # iterate through report's subsections
        if report.attrib['label']=='Current Cutout Values': 
            ccv_choice=report.record.attrib['choice_600_900_current'] # store choice cutout value
            ccv_select=report.record.attrib['select_600_900_current'] # store select cutout value
        if report.attrib['label']=='Beef Trimmings':
            weight=report.record.attrib['total_pounds'] # store weight
            price_low=report.record.attrib['price_range_low'] # store low price
            price_high=report.record.attrib['price_range_high'] # store high price
            wtd_avg=report.record.attrib['weighted_average'] # store weighted average
    data.append([date, ccv_choice, ccv_select, weight, price_low, price_high, wtd_avg]) # add data to list "data"
        
x=0
while x<len(data):
    # quandl data for current cutout values
    ccv_headings = ['Date', 'Choice Value', 'Select Value']
    ccv_data={'Date': [data[x][0]], 'Choice Value': [data[x][1]], 'Select Value': [data[x][2]]}
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
    bt_data={'Date': [data[x][0]], 'Weight': [data[x][3].replace(',','')], '$ Min': [data[x][4]], '$ Max': [data[x][5]], 'Weighted Average': [data[x][6]]}
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
    x=x+1