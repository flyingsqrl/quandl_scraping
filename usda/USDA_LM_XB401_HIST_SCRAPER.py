# -*- coding: utf-8 -*-
"""
Created on Thu Jun 12 12:27:52 2014

@author: nataliecmoore

Script Name: USDA_LM_XB401_HIST_SCRAPER

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
import datetime
import urllib2
import sys
from lxml import objectify

# Set the date range for the report (4 days to ensure all data is captured)
startdate = datetime.datetime.now() - 31 * pto.BDay()
startdate = startdate.strftime('%m/%d/%Y')

target_url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/beef/LM_XB401?filter={%22filters%22:[{%22fieldName%22:%22Report%20Date%22,%22operatorType%22:%22GREATER%22,%22values%22:[%2201/01/2014%22]}]}'

fileobj = urllib2.urlopen(target_url).read()

data=[]
root = objectify.fromstring(fileobj)
for report_date in root.report.iterchildren(): #processing must be repeated for each day covered by the report
    date = report_date.attrib['report_date']
    for report in report_date.iterchildren():
        if report.attrib['label']=='Central':
            for item in report.iterchildren():
                if item.attrib['item_desc']=='Chemical Lean, Fresh 90%':
                    
                    
                