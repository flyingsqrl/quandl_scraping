# -*- coding: utf-8 -*-
"""
Created on Tues June 03 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_GX_GR117_SCRAPER

Purpose:
Retrieve data for the Central Illinois Soymeal high and low offer
price.

Approach:
Found the minimum and maximum offer price for each soybean product
by finding the name of the product on the website and then using string parsing
to store the offer prices in a list. The list is then used to create a 
table and format the data for upload to Quandl.


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
import re


date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') #holds the date in YYYY-MM-DD format
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/gx_gr117.txt'
site_contents=urllib2.urlopen(url).read() 

labels=['Crude Soybean Oil', '48% Soybean Meal R', '48% Soybean Meal T', 'Soybean Hulls-bulk'] #names of each soybean product

x=0
# Loops though the relevant section of the website and finds the offer data
# for each soybean product in "labels"
while x<len(labels):
    first_index=site_contents.find(labels[x])
    end_index=site_contents.find(' ', site_contents.find('-', first_index+len(labels[x])))
    line=(site_contents[first_index+len(labels[x]):end_index]).strip().split('-') # splits the data so that the minimum offer price and maximum
                                                                                  # offer price are stored in different sections in "line"
    headings = [ 'Date', 'Low Offer Price', 'High Offer Price']
    data={'Date': [date], 'Low Offer Price': [line[0]], 'High Offer Price': [line[1]]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', labels[x]) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    quandl_code = 'USDA_GX_GR117_'+name2+'\n' # build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.\n'
    print 'code: ' + quandl_code+'\n'
    print 'name: Central Illinois Soybean Processor Report- '+labels[x]
    print 'description: This dataset contains the Central Illinois Soybean Processor Report for '+labels[x]+ \
    '\n  from the USDA GX_GR117 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). .\n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/gx_gr117.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1

