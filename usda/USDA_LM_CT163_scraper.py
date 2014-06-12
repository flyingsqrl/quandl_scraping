# -*- coding: utf-8 -*-
"""
Created on Fri Jun 06 11:44:37 2014

@author: nataliecmoore

Script Name: USDA_LM_CT163_SCRAPER

Purpose:
Retrieve TX-OK-NM weekly beef slaughter data from the LM_CT150 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for head count,
weight range, price range, average weight, and average price for steers and 
heifers of grade categories set by the USDA as well as the weekly accumulated
head count, average weight, and average price.

Approach:
Used pyparsing and nested loops to loop through each basis and beef grade type and
parse the line of data into a list. Then formatted the data into a table
that can be uploaded to Quandl.com.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
06/06/2014      Natalie Moore   Initial development/release

"""
from pyparsing import *
import urllib2
import pytz
import pandas as pd
import re
import datetime
import sys

# stores report in variable 'site_contents'
url='http://www.ams.usda.gov/mnreports/lm_ct163.txt'
site_contents=urllib2.urlopen(url).read()
date_begin=site_contents.find(', ', site_contents.find('Week Ending'))
date_end=site_contents.find('\r\n', date_begin)
first_break=site_contents.find('/', date_begin)
second_break=site_contents.find('/', first_break+1)
month=site_contents[date_begin+1:first_break].strip()
day=site_contents[first_break+1:second_break].strip()
year=site_contents[second_break+1:date_end].strip()
date=datetime.date(int(year), int(month), int(day)).strftime('%Y-%m-%d') # store date in YYYY-mm-dd format

basis_labels=['LIVE FOB BASIS', 'DRESSED DELIVERED BASIS']
type_labels=['STEERS', 'HEIFERS']
grade_labels=['Over 80% Choice', '65 - 80% Choice', '35 - 65% Choice', 
              '0 - 35% Choice', 'Total all grades']
              
# Loops through each basis type and uses the following loops to parse each line
# and find the head count, weight range, price range, average weight, and average price
# for each grade of beef
x=0
while x<len(basis_labels):
    start=site_contents.find(basis_labels[x])
    y=0
    # Loops through each type label (steer and heifer) and uses the loop inside
    # to loop through each choice to find the data values
    while y<len(type_labels):
        start_label=site_contents.find(type_labels[y], start)
        # Loops through each label in grade_labels and parses the line where
        # the label occurs. Then creates a table with the data and formats for
        # uploading
        z=0
        while z<len(grade_labels):
            start_choice=site_contents.find(grade_labels[z], start_label) # stores index of where choice_label is found 
            nonempty_line=Literal(grade_labels[z])+Word(nums+',')+Word(nums+','+'-'+'.')*4 # non empty line has name of choice_label and then 5 data values
            empty_line=Literal(grade_labels[z])+Literal('-')*2 # empty line is name of choice_label and then two hyphens
            line_grammar=nonempty_line | empty_line
            end_line=site_contents.find('\r\n', start_choice) # stores index of end of the line
            line=line_grammar.parseString(site_contents[start_choice:end_line]).asList() # parse line and convert to list
            if len(line)!=6: # if line is empty
                headings = [ 'Date', 'Head Count', 'Weight Low', 'Weight High','Wtd Avg Weight',\
                '$ Low','$ High', 'Wtd Avg Price']
                data={'Date': [date], 'Head Count': [0], 'Weight Low': [0], 'Weight High': [0],\
                'Wtd Avg Weight': [0], '$ Low': [0], '$ High': [0], 'Wtd Avg Price': [0]} # set each value to 0
            else: # when list isn't empty
                line=[g.split('-') for g in line] # split values with hyphens into individual data entries
                headings = [ 'Date', 'Head Count', 'Weight Low', 'Weight High','Wtd Avg Weight',\
                '$ Low','$ High', 'Wtd Avg Price']
                data={'Date': [date], 'Head Count': line[1][0], 'Weight Low': line[2][0], 'Weight High': line[2][1],\
                'Wtd Avg Weight': line[3][0],'$ Low': line[4][0], '$ High': line[4][1], 'Wtd Avg Price': line[5][0]}
            data_df = pd.DataFrame(data, columns = headings)
            data_df.index = data_df['Date']
            data_df = data_df.drop('Date', 1)
            replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
            remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
            name1 = replace.sub('_', grade_labels[z]) # replace certain characters with '_'
            name2 = remove.sub('', name1).upper() # remove certain characters and convert to upper case
            name2 = name2.translate(None, '-') # ensure '-' character is removed
            if x==0: # if data is for Live FOB Basis
                basis='LFB'
                basis_name='Live FOB'
            if x==1: # if data is for Dressed Delivered Basis
                basis='DDB'
                basis_name='Dressed Delivered'
            quandl_code = 'USDA_LM_CT163_' +basis+'_'+type_labels[y]+'_'+name2 # build unique quandl code
            reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
            '\n  at http://mpr.datamart.ams.usda.gov.\n' \
            '  All pricing is on a per CWT (100 lbs) basis.'
            print 'code: ' + quandl_code
            print 'name: TX-OK-NM Weekly Beef Slaughter- ' +basis_name+' '+type_labels[y].title()+' '+grade_labels[z]
            print 'description: Weekly head count, weight range, price range, average weight, and average price' \
            '\n  from the USDA LM_CT163 report published by the USDA Agricultural Marketing Service ' \
            '\n  (AMS). This dataset covers '+basis_name+' ' +type_labels[y].title() +" "+grade_labels[z] + '.\n'\
            + reference_text
            print 'reference_url: http://www.ams.usda.gov/mnreports/lm_ct163.txt'
            print 'frequency: daily'
            print 'private: false'
            print '---'
            data_df.to_csv(sys.stdout)
            print ''
            print ''
            z=z+1
        y=y+1
    x=x+1
    
labels=["Live    Steer", "Live    Heifer", "Dressed Steer", "Dressed Heifer"]
# Loops through each label in labels and uses pyparsing to find the weekly
# accumulated head count, average weight, and average price
x=0
while x<len(labels):
    nonempty_line=Literal(labels[x])+Word(nums+',')+Word(nums+','+'.')+Word(nums+'.'+'$') # grammar for non empty line
    empty_line=Literal(labels[x]) # Empty line has name of label and no data following
    line=nonempty_line | empty_line 
    start=site_contents.find(labels[x]) # stores index of beginning of label name
    end=site_contents.find('\r\n', start) # stores index of end of line
    parsed=line.parseString(site_contents[start:end]) # holds parsed line
    if len(parsed)!=1:    
        headings=['Date', 'Head Count', 'Average Weight', 'Average Price']
        data={'Date':[date], 'Head Count': [float(parsed[1].replace(',',''))], \
        'Average Weight': [float(parsed[2].replace(',',''))], 'Average Price': [float(parsed[3].replace('$',''))]}
    else:
        headings=['Date', 'Head Count', 'Average Weight', 'Average Price']
        data={'Date':[date], 'Head Count': [0], 'Average Weight': [0], 'Average Price': [0]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    name=" ".join(labels[x].split())
    quandl_code = 'USDA_LM_CT163_WEEKLY_'+name.replace(' ', '_').upper()
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'
    print 'code: ' + quandl_code
    print 'name: TX-OK-NM Weekly Accumulated Beef Slaughter- ' +name
    print 'description: Weekly accumulated head count, average weight, and average price' \
    '\n  from the USDA LM_CT163 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers ' +name + '.\n'\
    + reference_text
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_ct163.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    x=x+1
