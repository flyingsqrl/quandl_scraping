# -*- coding: utf-8 -*-
"""
Created on Fri Jun 06 10:04:17 2014

@author: nataliecmoore


Script Name: USDA_LM_CT150_SCRAPER

Purpose:
Retrieve weekly beef slaughter data from the LM_CT150 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for head count,
weight range, price range, average weight, and average price for steers and 
heifers of grade categories set by the USDA as well as the weekly weighted average
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
url='http://www.ams.usda.gov/mnreports/lm_ct150.txt'
site_contents=urllib2.urlopen(url).read()

date_begin=site_contents.find(', ', site_contents.find('Week Ending'))
date_end=site_contents.find('\r\n', date_begin)
first_break=site_contents.find('/', date_begin)
second_break=site_contents.find('/', first_break+1)
month=site_contents[date_begin+1:first_break].strip()
day=site_contents[first_break+1:second_break].strip()
year=site_contents[second_break+1:date_end].strip()
date=datetime.date(int(year), int(month), int(day)).strftime('%Y-%m-%d') # store date in YYYY-mm-dd format

basis_labels=['LIVE FOB BASIS', 'DRESSED DELIVERED BASIS', 'LIVE DELIVERED BASIS', 'DRESSED FOB BASIS']
type_labels=['STEERS', 'HEIFERS']
grade_labels=['Over 80% Choice', '65 - 80% Choice', '35 - 65% Choice', '0 - 35% Choice', 'Total all grades']
# Loops through each basis type and uses the following loops to parse each line
# and find the head count, weight range, price range, average weight, and average price
# for each grade of beef
x=0
while x<len(basis_labels):
    start_basis=site_contents.find(basis_labels[x])
    y=0
    # Loops through each type label (steer and heifer) and uses the loop inside
    # to loop through each choice to find the data values
    while y<len(type_labels):
        start_type=site_contents.find(type_labels[y], start_basis)
        # Loops through each label in choice_labels and parses the line where
        # the label occurs. Then creates a table with the data and formats for
        # uploading
        z=0
        while z<len(grade_labels):
            start_grade=site_contents.find(grade_labels[z], start_type)
            end_grade=site_contents.find('\r\n', start_grade)
            empty_line=Literal(grade_labels[z])+Literal('-')*2
            nonempty_line=Literal(grade_labels[z])+Word(nums+','+'-'+'.')*5
            line=nonempty_line | empty_line
            line=line.parseString(site_contents[start_grade:end_grade])
            if len(line)!=6: # if line is empty
                headings = [ 'Date', 'Head Count', 'Weight Low', 'Weight High','$ Low',\
                '$ High', 'Avg Weight', 'Avg Price']
                data={'Date': [date], 'Head Count': [0], 'Weight Low': [0], 'Weight High': [0],\
                '$ Low': [0], '$ High': [0], 'Avg Weight': [0], 'Avg Price': [0]} # set each value to 0
            else: # when list isn't empty
                line=[g.split('-') for g in line] # split values with hyphens into individual data entries
                headings = [ 'Date', 'Head Count', 'Weight Low', 'Weight High','$ Low', '$ High', 'Avg Weight', 'Avg Price']
                data={'Date': [date], 'Head Count': line[1][0], 'Weight Low': line[2][0], 'Weight High': line[2][1],\
                '$ Low': line[3][0],'$ High': line[3][1], 'Avg Weight': line[4][0], 'Avg Price': line[5][0]}
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
            if x==2: # if data is for Live FOB Basis
                basis='LDB'
                basis_name='Live Delivered'
            if x==3: # if data is for Dressed Delivered Basis
                basis='DFB'
                basis_name='Dressed FOB'
            quandl_code = 'USDA_LM_CT150_' +basis+'_'+type_labels[y]+'_'+name2 # build unique quandl code
            reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
            '\n  at http://mpr.datamart.ams.usda.gov.\n' \
            '  All pricing is on a per CWT (100 lbs) basis.'
            print 'code: ' + quandl_code
            print 'name: Five Area Weekly Beef Slaughter- ' +basis_name+" "+type_labels[y].title()+" "+grade_labels[z]
            print 'description: Five area weekly head count, weight range, price range, average weight, and average price' \
            '\n  from the USDA LM_CT150 report published by the USDA Agricultural Marketing Service ' \
            '\n  (AMS). This dataset covers '+basis_name+' ' +type_labels[y].title() +" "+grade_labels[z] + '.\n'\
            + reference_text
            print 'reference_url: http://www.ams.usda.gov/mnreports/lm_ct150.txt'
            print 'frequency: daily'
            print 'private: false'
            print '---'
            data_df.to_csv(sys.stdout)
            print ''
            print ''
            z=z+1
        y=y+1
    x=x+1
    
    
labels=['Live FOB Steer', 'Live FOB Heifer', 'Dressed Del Steer', 'Dressed Del Heifer']
# Loops through each label in labels and uses pyparsing to find the weekly
# accumulated head count, average weight, and average price
x=0
while x<len(labels):
    nonempty_line=Literal(labels[x])+Word(nums+',')+Word(nums+','+'.')+Word(nums+'.') # grammar for non empty line
    empty_line=Literal(labels[x]) # Empty line has name of label and no data following
    line=nonempty_line | empty_line 
    start=site_contents.find(labels[x]) # stores index of beginning of label name
    end=site_contents.find('\r\n', start) # stores index of end of line
    parsed=line.parseString(site_contents[start:end]) # holds parsed line
    headings=['Date', 'Head Count', 'Average Weight', 'Average Price']
    data={'Date':[date], 'Head Count': [float(parsed[1].replace(',',''))], \
    'Average Weight': [float(parsed[2].replace(',',''))], 'Average Price': [float(parsed[3].replace(',',''))]}
    data_df = pd.DataFrame(data, columns = headings)
    data_df.index = data_df['Date']
    data_df = data_df.drop('Date', 1)
    name=" ".join(labels[x].split())
    quandl_code = 'USDA_LM_CT150_WEEKLY_'+name.replace(' ', '_').upper()
    reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'
    print 'code: ' + quandl_code
    print 'name: Five Area Weekly Accumulated Beef Slaughter- ' +name
    print 'description: Weekly weighted averaged head count, average weight, and average price' \
    '\n  from the USDA LM_CT150 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers ' +name + '.\n'\
    + reference_text
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_ct150.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    data_df.to_csv(sys.stdout)
    print ''
    print ''
    x=x+1