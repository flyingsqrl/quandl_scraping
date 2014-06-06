# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 08:00:02 2014

@author: nataliecmoore

Script Name: USDA_LM_HG201_SCRAPER

Purpose:
Retrieve daily USDA hog slaughter data from the LM_HG201 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for
producer sold, packer sold, and packer owned hogs.

Approach:
Used pyparsing to make a list of the relevant data for each of the categories for 
producer sold, packer sold, and packer owned hogs. A table was made for each
category and formatted for upload to Quandl.com.


Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/04/2014      Natalie Moore   Initial development/release

"""

from pyparsing import *
import urllib2
import pytz
import pandas as pd
import datetime 
import sys
import re

date=datetime.datetime.now(pytz.timezone('US/Eastern')) # holds the date in YYYY-MM-DD format
date=(date-datetime.timedelta(hours=24)).strftime("%Y-%m-%d") # find the previous day's date (because report is prior day)

# stores report in variable "site_contents"
url="http://www.ams.usda.gov/mnreports/lm_hg201.txt"
site_contents=urllib2.urlopen(url).read()

labels= ["Producer Sold", "Packer Sold", "Packer Owned"]  #labels for each of the main categories
# The following three lists hold the labels for each sub-category
labels_prod_sold=["HEAD COUNT", "CARCASS BASE PRICE", "AVERAGE NET PRICE", "LOWEST NET LOT", "HIGHEST NET LOT", \
        "AVERAGE LIVE WT", "AVERAGE CARCASS WT", "AVERAGE SORT LOSS", "AVERAGE BACKFAT", "AVERAGE LOIN DEPTH (LD)", \
        "LOINEYE AREA (LD Converted)", "AVERAGE LEAN PERCENT"]
labels_pack_sold=["HEAD COUNT", "CARCASS BASE PRICE", "AVERAGE NET PRICE", "AVERAGE OF LOWEST NET LOTS", \
                  "AVERAGE OF HIGHEST NET LOTS", "AVERAGE LIVE WT", "AVERAGE CARCASS WT", "AVERAGE SORT LOSS", \
                  "AVERAGE BACKFAT", "AVERAGE LOIN DEPTH (LD)", "LOINEYE AREA (LD Converted)", "AVERAGE LEAN PERCENT"]
labels_pack_owned=["HEAD COUNT", "AVERAGE LIVE WT", "AVERAGE CARCASS WT", "AVERAGE BACKFAT", "AVERAGE LOIN DEPTH (LD)", "AVERAGE LEAN PERCENT"]
#store all of the section labels in one list
section_labels=[labels_prod_sold, labels_pack_sold, labels_pack_owned]

# Loops through each label and creates a quandl dataset for each subcategory in the label category
x=0
while x<len(labels):
    starting_index=site_contents.find(labels[x])   # index of where the section begins 
    parsed=[]
    # Loops through each of the sub-category labels, parses each line, and stores the data in parsed    
    s=0
    while s<len(section_labels[x]):
        if x==0:      # if on first label, 5 data points need to be stored for each sub-category                                              
            line_grammar=(section_labels[x][s]+Word(printables)*5)
        else:         # if on either of the remaining labels, only 1 data point needs to be stored
            line_grammar=(section_labels[x][s]+Word(printables))       
        start_section_index=site_contents.find(section_labels[x][s], starting_index) # index of where line begins
        ending_index=site_contents.find("\r\n", start_section_index) # index where line ends
        parsed.append(line_grammar.parseString(site_contents[start_section_index:ending_index])) # parse line and store in list
        parsed[s]=[float(y.replace(",","")) for y in parsed[s][1:]] #remove commas and convert to float
        s=s+1
    # Loops through each line in "parsed" and creates a quandl data set for each line of data    
    b=0
    while b<len(parsed):         
        if len(parsed[0])!=1: # If on first label, use following headings and data
            headings = [ 'Date', 'Negotiated', 'Other Market Formula', 'Swine or Pork Market Formula', 'Other Purchase Arrgment', 'Totals/Wtd Avg']
            data={'Date': [date], 'Negotiated': [parsed[0]], 'Other Market Formula': [parsed[1]], 'Swine or Pork Market Formula': [parsed[2]], \
            'Other Purchase Arrgment': [parsed[3]], 'Totals/Wtd Avg': [parsed[4]]}
        else: # If on other two, use following headings and data
            headings= ['Date', 'Totals/Wtd Avg']
            data={'Date': [date], 'Totals/Wtd Avg': parsed[b]}
        replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
        remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
        name1 = replace.sub('_', labels[x].upper()+'_'+section_labels[x][b]) # replace certain characters with '_'
        name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
        name2 = name2.translate(None, '-') # ensure '-' character is removed
        data_df = pd.DataFrame(data, columns = headings)
        data_df.index = data_df['Date']
        data_df = data_df.drop('Date', 1)
        quandl_code = 'USDA_LM_HG201_'+name2+'\r'# build unique quandl code
        reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
        '\n  at http://mpr.datamart.ams.usda.gov.\n' 
        print 'code: ' + quandl_code+'\n'
        print 'name: National Slaughtered Hog Data- '+labels[x]+"- "+section_labels[x][b].title()+'\n'
        print 'description: National daily direct slaughtered swine report for '+labels[x].lower()+' hogs. This report contains the '+section_labels[x][b].lower()+\
        ' from the USDA LM_HG201 report published by the USDA Agricultural Marketing Service ' \
        '(AMS).\n'\
        + reference_text+'\n'
        print 'reference_url: http://www.ams.usda.gov/mnreports/lm_hg201.txt\n'
        print 'frequency: daily\n'
        print 'private: false\n'
        print '---\n'
        data_df.to_csv(sys.stdout)
        print '\n'
        print '\n'
        b=b+1
    x=x+1
        
        
