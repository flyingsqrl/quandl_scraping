# -*- coding: utf-8 -*-
"""
Created on Tues Jun 03 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_AJ_PY018_SCRAPER

Purpose:
Find the Georgia f.o.b. dock quoted price on broilers/fryers


Approach:
Found the relevant section of the website and used python string parsing
to find the quoted price.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/03/2014      Natalie Moore   Initial development/release

"""

import urllib2
import pytz
import pandas as pd
import datetime 
import sys


date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') # holds the date in YYYY-MM-DD format
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/aj_py018.txt'
site_contents=urllib2.urlopen(url).read()

decimal=site_contents.rfind('.', 0, site_contents.find('based'))            # find the decimal point in the price
space_before=site_contents.rfind(' ', 0, decimal)                           # find the space before the price
space_after=site_contents.find(' ', decimal)                                # find the space after the price
dock_quoted_price=float(site_contents[space_before:space_after].strip())    # store the quoted price as a float and remove extra spaces around it

headings=['Date', 'Quoted Price (cents)']
data={'Date': [date], 'Quoted Price (cents)': [dock_quoted_price]}
data_df = pd.DataFrame(data, columns = headings)
data_df.index = data_df['Date']
data_df = data_df.drop('Date', 1)
quandl_code = 'USDA_AJ_PY018_BROILER_FRYER_QUOTED_PRICE\r'# build unique quandl code
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' 
print 'code: ' + quandl_code+'\n'
print 'name: Georgia F.O.B. Dock Broiler/Fryer Quoted Price\n'
print 'description: Georgia F.O.B. Dock quoted price on broilers/fryers'\
'\n  from the USDA AJ_PY018 report published by the USDA Agricultural Marketing Service ' \
'\n  (AMS).\n'\
+ reference_text+'\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/AJ_PY018.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
data_df.to_csv(sys.stdout)
print '\n'
print '\n'
