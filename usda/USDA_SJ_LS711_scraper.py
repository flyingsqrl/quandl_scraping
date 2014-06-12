# -*- coding: utf-8 -*-
"""
Created on Fri May 30 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_SJ_LS711_SCRAPER

Purpose:
Retrieve weekly USDA beef slaughter data from the USDA LMR
web service for upload to Quandl.com. The script pulls data for
Monday-Saturday cattle slaughter, weight of beef production, live weight
and dressed weight for cattle, and the number of head slaughtered.

Approach:
Used string parsing and pyparsing to find the relevant data on the website
and format into a table.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/03/2014      Natalie Moore   Initial development/release

"""

from pyparsing import *
import urllib2
import pytz
import pandas as pd
import re
import datetime 
import sys
import calendar


# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/sj_ls711.txt'
site_contents=urllib2.urlopen(url).read()

day=[]
number_cattle_slaughter=[]

date_grammar=Word(alphas)+Word(nums)+Suppress(Literal(','))+Word(nums) # grammar for find the date of the report


begin=site_contents.find(',', site_contents.find('Week Ending'))+2 # stores index where date begins
end=site_contents.find('\r\n', begin) # stores index of end of date
date=date_grammar.parseString(site_contents[begin:end]) # parse date to find month, day, and year
date=datetime.date(int(date[2]), list(calendar.month_name).index(date[0]), int(date[1])) # create a datetime object 
                                                                                         # with the previously parsed string
starting_day=site_contents.find('Monday', site_contents.find('Bison 1/')) # store index of beginning of first data section
ending_day=site_contents.find('---', starting_day) # store index of end of first data section
section=site_contents[starting_day:ending_day] # store data section

unparsed=[] # list that will later hold unparsed lines
parsed=[] # list that will later hold parsed lines
# Loops through first section of data and finds the slaughter for each day
while site_contents.find('\r\n', starting_day, ending_day)!=-1:
    end_line=site_contents.find('\r\n', starting_day)
    unparsed.append(site_contents[starting_day:end_line])
    starting_day=end_line+2
cattle_slaughter=Word(alphas)+Word(printables) | Word(alphas)+Literal("-") # grammar for daily cattle slaughter
# Loops through each unparsed line and parses it, appending to number_cattle_slaughter
x=0
while x<len(unparsed):
    parsed.append(cattle_slaughter.parseString(unparsed[x]))
    day.append(parsed[x][0])
    number_cattle_slaughter.append(parsed[x][1])
    x=x+1
number_cattle_slaughter=[float(g.replace(',','')) for g in number_cattle_slaughter] # remove commas and convert each value to a float

starting=site_contents.find('Beef', site_contents.find('Meat Production, Live Weight and Dressed Weight')) # store index of beginning of second data section
weight=Suppress(Literal('Beef'))+Word(printables) # grammar for finding weight of beef production
weight=float((weight.parseString(site_contents[starting:]))[0])
starting=site_contents.find('Cattle', starting)
word=Suppress(Word(alphas))+Word(nums)
weights=Suppress(Word(alphas))+Word(printables)+Word(nums)+word*4
average_live_weight=float((weights.parseString(site_contents[starting:]))[0].replace(',',''))
average_dressed_weight=float((weights.parseString(site_contents[starting:]))[1])
average_weights=weights.parseString(site_contents[starting:])[2:]

starting=site_contents.find('Cattle', site_contents.find('Federally Inspected Slaughter Head & Percentage by Class'))
suppress=Suppress(Word(printables))
word=Word(alphas)+ZeroOrMore(Word(alphas))
number=Word(printables)
cattle_head=suppress+(word+number)+(word+number+suppress)*5
y=cattle_head.parseString(site_contents[starting:])
names=[y[0], y[2], y[4], y[6]+' '+y[7], y[9]+' '+y[10], y[12]]
total_head=[y[1],y[3],y[5],y[8], y[11], y[13]]
total_head=[float(i.replace(',','.')) for i in total_head]

dates=[]
x=5
while x>=0:
    dates.append((date-datetime.timedelta(hours=24*x)).isoformat())
    x=x-1
    
headings = [ 'Date', 'Actual Cattle Slaughter']
data={'Date': dates, 'Actual Cattle Slaughter': number_cattle_slaughter}
data_df = pd.DataFrame(data, columns = headings)
data_df.index = data_df['Date']
data_df = data_df.drop('Date', 1)
quandl_code = 'USDA_SJ_LS711_ACTUAL_CATTLE_SLAUGHTER\n' # build unique quandl code
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' \
'  All pricing is on a per CWT (100 lbs) basis.\n'
print 'code: ' + quandl_code+'\n'
print 'name: Actual Cattle Slaughter Under Federal Inspection\n '
print 'description: Actual Cattle Slaughter\n ' \
'\n  from the USDA SJ_LS711 report published by the USDA Agricultural Marketing Service ' \
'\n  (AMS). \n'\
+ reference_text+'\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
data_df.to_csv(sys.stdout)
print '\n'
print '\n'

    
headings = [ 'Date', 'FI Beef Production (million pounds)']
data={'Date': [date], 'FI Beef Production (million pounds)': [weight]}
data_df = pd.DataFrame(data, columns = headings)
data_df.index = data_df['Date']
data_df = data_df.drop('Date', 1)
quandl_code = 'USDA_SJ_LS711_FI_BEEF_PRODUCTION\n' # build unique quandl code
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' \
'  All pricing is on a per CWT (100 lbs) basis.\n'
print 'code: ' + quandl_code+'\n'
print 'name: FI Beef Production\n '
print 'description: Weekly beef production (million pounds) ' \
'\n  from the USDA SJ_LS711 report published by the USDA Agricultural Marketing Service ' \
'\n  (AMS). \n'\
+ reference_text+'\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
data_df.to_csv(sys.stdout)
print '\n'
print '\n'


headings = [ 'Date', 'Average Live Weight', 'Average Dressed Weight']
data={'Date': [date], 'Average Live Weight': [average_live_weight], \
      'Average Dressed Weight':[average_dressed_weight]}
data_df = pd.DataFrame(data, columns = headings)
data_df.index = data_df['Date']
data_df = data_df.drop('Date', 1)
quandl_code = 'USDA_SJ_LS711_FI_BEEF_AVERAGE_WEIGHTS\n' # build unique quandl code
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
'\n  at http://mpr.datamart.ams.usda.gov.\n' \
'  All pricing is on a per CWT (100 lbs) basis.\n'
print 'code: ' + quandl_code+'\n'
print 'name: FI Beef Production- Average Weights\n '
print 'description: Average live weight and average dressed weight of federally inspected beef ' \
'\n  from the USDA SJ_LS711 report published by the USDA Agricultural Marketing Service ' \
'\n  (AMS). \n'\
+ reference_text+'\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
data_df.to_csv(sys.stdout)
print '\n'
print '\n'

name_labels=['Steers', 'Heifers', 'Cows', 'Bulls']
x=0
while x<len(average_weights):
    headings = [ 'Date', 'Average Weight']
    data={'Date': [date], 'Average Weight': [average_weights[x]]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    quandl_code = 'USDA_SJ_LS711_AVG_WEIGHT_'+name_labels[x].upper() # build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.\n'
    print 'code: ' + quandl_code+'\n'
    print 'name: FI Beef Production- '+name_labels[x]+' Average Weight\n '
    print 'description: Average weight of federally inspected '+name_labels[x]+ \
    '\n  from the USDA SJ_LS711 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). \n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1
    
x=0
while x<len(total_head):
    headings = [ 'Date', 'Total Head (000)']
    data={'Date': [date], 'Total Head (000)': [total_head[x]]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    quandl_code = 'USDA_SJ_LS711_HEAD_'+names[x].upper().replace(' ','_') # build unique quandl code
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.\n'
    print 'code: ' + quandl_code+'\n'
    print 'name: FI Beef Production- '+names[x].title()+' Total Head'
    print 'description: Number of head of federally inspected beef '+ \
    '\n  from the USDA SJ_LS711 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). \n'\
    + reference_text+'\n'
    print 'reference_url: http://www.ams.usda.gov/mnreports/sj_ls711.txt\n'
    print 'frequency: daily\n'
    print 'private: false\n'
    print '---\n'
    data_df.to_csv(sys.stdout)
    print '\n'
    print '\n'
    x=x+1

