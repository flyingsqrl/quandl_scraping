# -*- coding: utf-8 -*-
"""
Created on Tue Jun 10 10:34:50 2014

@author: nataliecmoore

Script Name: USDA_LM_PK602_SCRAPER

Purpose:
Retrieve daily USDA pork data from from the LM_PK602 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for
volume, cutout/primal values and data for each individual cut of pork tracked by
USDA.

Approach:
Used pyparsing to find weight, min cost, max cost, and weighted average
for each pork cut. Made a csv table for each cut and formatted to upload to Quandl.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/10/2014      Natalie Moore   Initial development/release

"""
from pyparsing import *
import urllib2
import pytz
import pandas as pd
import datetime 
import sys
import re


date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d') # holds the date in YYYY-MM-DD format
# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/lm_pk602.txt'
site_contents=urllib2.urlopen(url).read()

reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'

# contains a list of each primal cut in the report
labels=["Loin", "Butt", "Picnic", "Sparerib", "Ham", "Belly", "Jowl", "Trim", "Variety", "AI (Added Ingreds)"]
start=site_contents.find("Loin", site_contents.find("Five Day Average"))
x=0
# Loops through each primal cut in labels and makes a data table for 
# each of the cuts contained under the primal cut heading in the report
while x<len(labels):
    start=site_contents.find(labels[x], start) 
    line_end=start
    # Loops through each cut and uses pyparsing to find the data associated with
    # that cut. Creates a data table for the cut and uses the parsed line to fill in data.
    while site_contents.find('---', line_end)!=line_end+2:
        cut_start=site_contents.find(" ", site_contents.find("\r\n", start)) # find where name of cut begins
        cut_end=site_contents.find("  ", cut_start+1) # find where name of cut ends
        line_end=site_contents.find("\r\n", cut_end) # find end of line for cut
        name=site_contents[cut_start:cut_end].strip() # store name of cut
        float_num=Word(nums+'.') 
        nonempty_line=Literal(name)+Word(nums+',')+float_num+Suppress(Literal('-'))+float_num*2 
        empty_line=Literal(name)+Literal('-') # an empty line has name of cut with a hyphen following
        line=nonempty_line | empty_line    # line is either empty or nonempty
        parsed=line.parseString(site_contents[cut_start:line_end]) # parse line of data following cut name
        start=line_end # the next start becomes the line end
        cuts_headings = [ 'Date', 'Primal', 'LBS', '$ Low', '$ High', '$ WgtAvg']
        if len(parsed)!=2:
            weight=float(parsed[1].replace(',','')) 
            min_cost=float(parsed[2])
            max_cost=float(parsed[3])
            wtd_avg=float(parsed[4])
            data={'Date': [date], 'Primal': [labels[x]], 'LBS': [weight], '$ Low': [min_cost],\
            '$ High': [max_cost], '$ WgtAvg': [wtd_avg]}
        else:
            data={'Date': [date], 'Primal': [labels[x]], 'LBS': [0], '$ Low': [0],\
            '$ High': [0], '$ WgtAvg': [0]}
        cuts_df = pd.DataFrame(data, columns = cuts_headings)
        cuts_df.index = cuts_df['Date']
        cuts_df = cuts_df.drop('Date', 1).drop('Primal', 1)
        replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
        remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
        cut1 = replace.sub('_', name) # replace certain characters with '_'
        cut2 = remove.sub('', cut1).upper() # remove certain characters and convert to upper case
        cut2 = cut2.translate(None, '-') # ensure '-' character is removed
        quandl_code = 'USDA_LM_PK602_' + cut2 # build unique quandl code
        print 'code: ' + quandl_code
        print 'name: Pork ' +labels[x]+" Cuts"+' - '+name.title()
        print 'description: Daily total pounds, low price, high price and weighted average price ' \
        '\n  from the USDA LM_PK602 report published by the USDA Agricultural Marketing Service ' \
        '\n  (AMS). This dataset covers ' + name.title() + '.\n'\
        + reference_text
        print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
        print 'frequency: daily'
        print 'private: false'
        print '---'
        cuts_df.to_csv(sys.stdout)
        print ''
        print ''
    x=x+1


#Finds the daily pork volume (full loads and trim/process loads)
d_tl_pork=site_contents.find(':', site_contents.find('Loads PORK CUTS'))
d_tl_pork=site_contents[d_tl_pork+6:site_contents.find('\r\n', d_tl_pork+6)]
d_tp_pork=site_contents.find(':', site_contents.find('Loads TRIM/PROCESS PORK'))
d_tp_pork=site_contents[d_tp_pork+6:site_contents.find('\r\n', d_tp_pork+6)]


#Finds the daily pork cutout and primal values
primal_cutout_list=[]
starting_index=site_contents.find('\r\n', site_contents.find('\r\n', site_contents.find("Belly"))+1)
ending_index=site_contents.find('\r\n', starting_index+1)
row=site_contents[starting_index+17:ending_index]
primal_row=re.compile('\s\s+').split(row)

# volume_df holds the daily shipment volume information
volume_headings = ['Date', 'Total Loads', 'Trim/Process Loads']
loads={'Date': [date], 'Total Loads': [d_tl_pork], 'Trim/Process Loads': [d_tp_pork]}
volume_df = pd.DataFrame(loads, columns = volume_headings)
volume_df.index = volume_df['Date']
volume_df.drop(['Date'],inplace=True,axis=1) 

# Print quandl dataset for VOLUME
print 'code: USDA_LM_PK602_VOLUME\n'
print 'name: Daily pork volume (full loads and trim/process loads)\n'
print 'description: "Daily pork volume (full loads and trim/process loads) from the USDA LM_PK602\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text + '"\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
volume_df.to_csv(sys.stdout)
print '\n'
print '\n'

# primal_df holds the daily cutout and primal data  
primal_cutout=['Date','Carcass Value', 'Loin Value', 'Butt Value', 'Picnic Value', 'Rib Value', 'Ham Value', 'Belly Value'] 
primal_loads={'Date': [date], 'Carcass Value': [primal_row[1]], 'Loin Value': [primal_row[2]], 'Butt Value': [primal_row[3]], 'Picnic Value': [primal_row[4]], 'Rib Value': [primal_row[5]],\
 'Ham Value': [primal_row[6]], 'Belly Value': [primal_row[7]]}
primal_df = pd.DataFrame(primal_loads, columns=primal_cutout)
primal_df.index = primal_df['Date']
primal_df.drop(['Date'],inplace=True,axis=1)

# Print quandl dataset for CUTOUT AND PRIMAL VALUES
print 'code: USDA_LM_PK602_CUTOUT_PRIMAL'
print 'name: Daily USDA pork cutout and primal values'
print 'description: Daily pork cutout and primal values from the USDA LM_PK602\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
print 'frequency: daily'
print 'private: false'
print '---'
primal_df.to_csv(sys.stdout)
print ''
print ''
