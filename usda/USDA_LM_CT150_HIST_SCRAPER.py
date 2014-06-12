# -*- coding: utf-8 -*-
"""
Created on Thu Jun 12 09:26:40 2014

@author: nataliecmoore

Script Name: USDA_LM_CT150_HIST_SCRAPER

Purpose:

Approach:

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/12/2014      Natalie Moore   Initial development/release

"""

import pandas as pd
import pandas.tseries.offsets as pto
import re
import datetime
import urllib2
import sys
from lxml import objectify

# Set the date range for the report (4 days to ensure all data is captured)
startdate = datetime.datetime.now() - 31 * pto.BDay()
startdate = startdate.strftime('%m/%d/%Y')

target_url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/cattle/LM_CT150?filter={%22filters%22:[{%22fieldName%22:%22Report%20Date%22,%22operatorType%22:%22GREATER%22,%22values%22:[%2201/01/2014%22]}]}'

fileobj = urllib2.urlopen(target_url).read()

data=[]

root = objectify.fromstring(fileobj)

for report_date in root.report.iterchildren(): #processing must be repeated for each day covered by the report
    date = report_date.attrib['report_date']
    for report in report_date.iterchildren():
        if report.attrib['label']=='Detail':
            for item in report.iterchildren():

                data.append([date, item.attrib['class_description'], item.attrib['selling_basis_description'],\
                             item.attrib['grade_description'], item.attrib['head_count'], item.attrib['weight_range_low'], \
                             item.attrib['weight_range_high'], item.attrib['price_range_low'],\
                             item.attrib['price_range_high'], item.attrib['weight_range_avg'], \
                             item.attrib['weighted_avg_price']])
                        
x=0
while x<len(data):
    month=data[x][0][0:2]
    day=data[x][0][3:5]
    year=data[x][0][6:]
    date=datetime.date(int(year), int(month), int(day))
    date=(date-datetime.timedelta(hours=24)).strftime('%Y-%m-%d')
    headings = [ 'Date', 'Head Count', 'Weight Low', 'Weight High','$ Low', '$ High', 'Avg Weight', 'Avg Price']
    ct_data={'Date': [date], 'Head Count': [data[x][4].replace(',','')], 'Weight Low': [data[x][5].replace(',','')], 'Weight High': [data[x][6].replace(',','')],\
    '$ Low': [data[x][7]],'$ High': [data[x][8]], 'Avg Weight': [data[x][9].replace(',','')], 'Avg Price': [data[x][10]]}
    data_df = pd.DataFrame(ct_data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', data[x][3]) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    if data[x][2]=="LIVE DELIVERED":
        basis='LDB'
    if data[x][2]=='DRESSED DELIVERED':
        basis='DDB'
    if data[x][2]=='LIVE FOB':
        basis='LFB'
    if data[x][2]=='DRESSED FOB':
        basis='DFB'
    quandl_code = 'USDA_LM_CT150_' +basis+'_'+(data[x][1]+'s').upper()+'_'+name2 # build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'
    print 'code: ' + quandl_code  
    print 'name: Five Area Weekly Beef Slaughter- ' +data[x][2].title()+" "+(data[x][1]+'s').title()+" "+data[x][3]
    print 'description: Five area weekly head count, weight range, price range, average weight, and average price' \
    '\n  from the USDA LM_CT150 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers '+data[x][2].title()+' ' +(data[x][1]+'s').title() +" "+data[x][3] + '.\n'\
    + reference_text
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_ct150.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    x=x+1