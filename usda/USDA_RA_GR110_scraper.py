# -*- coding: utf-8 -*-
"""
Created on Tues Jun 03 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_RA_GR110_SCRAPER

Purpose:
Acquire the soymeal data for Fayetteville, NC and Raleigh, NC

Approach:
Found the section of the website where data will be extracted from and 
divided so that pyparsing would extract the dollars per ton of soymeal
data for each city. The data was then put into a table and formatted for
upload to Quandl.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/03/2014      Natalie Moore   Initial development/release

"""
from pyparsing import Word, ZeroOrMore, alphas, printables
import urllib2
import pytz
import pandas as pd
import datetime 
import sys
import re

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') # holds the date in YYYY-MM-DD format
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/ra_gr110.txt'
site_contents=urllib2.urlopen(url).read()

grains=['US 2 Yellow Corn', 'US 1 Yellow Soybeans', 'US 2 Soft Red Winter Wheat']


# Loops through each grain in "grains" and finds the elevator price data. 
# The minimum and maximum price are stored in a table and formatted for upload to Quandl.
x=0
while x<len(grains):
    begin=site_contents.find(grains[x]) # the beginning of each section is where each grain name is found                                         
    end=site_contents.rfind('at', begin, site_contents.find('elevators.', begin)) #the end of each section is right before the phrase "at the elevators"
    hyphen=site_contents.rfind('-', begin, end) # hyphen separates the minimum and maximum price
    space_before=site_contents.rfind(' ', begin, hyphen) #index of space before minimum price
    space_after=site_contents.find(' ', hyphen) # index of space after maximum price
    minimum=site_contents[space_before:hyphen].replace('\r\n', '').strip() 
    maximum=site_contents[hyphen+1:end].replace('\r\n', '').strip()
    headings=[ 'Date', 'Minimum Price', 'Maximum Price']
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    name1 = replace.sub('_', grains[x]) # replace certain characters with '_'
    name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
    name2 = name2.translate(None, '-') # ensure '-' character is removed
    data={'Date': [date], 'Minimum Price': [minimum], 'Maximum Price': [maximum]}
    data_df=pd.DataFrame(data, columns=headings)
    data_df.index=data_df['Date']
    data_df=data_df.drop('Date', 1)
    quandl_code='USDA_RA_GR110_'+name2+'\r'
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'code: ' + quandl_code+'\n'
    print 'name: North Carolina '+grains[x]+' Prices at Elevators\n'
    print 'description: North Carolina '+grains[x]+ ' prices paid to producers' \
    ' from the USDA RA_GR110 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS).\n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/ra_gr110.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1

starting_index=site_contents.find('Fayetteville', site_contents.find('Soybean Processors')) # starting index of section of website to gather data from
ending_index=site_contents.find('Source', starting_index) # ending index of section of website to gather data from

city_names=['FAYETTEVILLE', 'RALEIGH']                                  # names of two cities
city_list=[]                                                            # will hold data for each city
line=Word(alphas)+ZeroOrMore(Word(printables))                          # line starts with a city name and is either followed by numbers or no data
break_point=site_contents.find('\r\n', starting_index)                  
fayetteville=line.parseString(site_contents[starting_index:break_point])# fayetteville data is from the city name to the end of the line 
raleigh=line.parseString(site_contents[break_point:ending_index])       # raleigh data is from the end of the previous line to the end of the website section
fayetteville.insert(1, 0)                                               # insert 0 after city name in case there is no data for that city
raleigh.insert(1, 0)
city_list.append(fayetteville)                                          # add city data to city_list
city_list.append(raleigh)

x=0
while x<2:
    headings = [ 'Date', 'Meal (48% Protein) Dollars per ton']
    data={'Date': [date], 'Meal (48% Protein) Dollars per ton': [city_list[x][len(city_list[x])-1]]}  # the needed data is always at the last index in the city's data
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    quandl_code = 'USDA_RA_GR110_'+ city_names[x]+'\r'# build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' 
    print 'code: ' + quandl_code+'\n'
    print 'name: North Carolina Soymeal Prices- '+city_list[x][0]
    print 'description: Daily soybean production (dollars per ton) in '+city_list[x][0]+ ', North Carolina' \
    '\n  from the USDA RA_GR110 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS).\n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/ra_gr110.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1