# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary


Script Name: USDA_LM_PK602_SCRAPER

Purpose:
Retrieve daily USDA pork data from from the LM_PK602 report via the USDA LMR
web service for upload to Quandl.com.  This script is designed to access the LMR
web service and pull data for the previous five days.  The script pulls data for
volume, cutout/primal values and data for each individual cut of pork tracked by
USDA.

Approach:
Access LMR web service and retrieve XML output.  Use lxml library to parse output.
Output for each section of the report is written to python lists, which are then
converted to pandas dataframes for further manipulation.  From the dataframes, write
the data to the screen (along with metadata) in csv format for consumption by quandl.com.

Author: Bill Cary

History:

Date            Author      Purpose
-----           -------     -----------
05/23/2014      Bill Cary   Initial development/release

"""

import pandas as pd
import sys
import re
import datetime as dt
import urllib2
from dateutil import parser



# List of categories of pork cuts for use later in routine
pork_cuts= ['Loin Cuts', 'Butt Cuts', 'Picnic Cuts', 'Ham Cuts', 'Belly Cuts', \
    'Sparerib Cuts', 'Jowl Cuts', 'Trim Cuts', 'Variety Cuts', \
    'Added Ingredient Cuts']

# Initialize empty python lists to hold data to be extracted from the XML
cuts = [] #"generic" list to store the USDA market data for individual cuts
primal_cutout = [] # list to store primal cutout values
loads = [] # list to store the number of loads comprising the market for the day

# Access and open the URL for the report
target_url = 'http://www.ams.usda.gov/mnreports/lm_pk602.txt'
fileobj = urllib2.urlopen(target_url)

# Initialize a list to hold the entire URL and the relevant lines of data
url_lines = []
for line in fileobj:
    url_lines.append(line)
    
# Extract week ending date from report (contained on 3rd line of file)
report_dt = url_lines[2].strip()[33:45]

report_dt = parser.parse(report_dt)

report_dt = pd.to_datetime(report_dt)   

'''
Each of the three sections of the report (primal/cutout, volume, indivudual pork cuts)
are placed into pandas dataframes to allow easy manipulation into the format required of
a quandl csv file.
'''
primal_headings = ['Date', 'Carcass Value', 'Loin Value', 'Butt Value', 'Picnic Value', 'Rib Value', 'Ham Value', 'Belly Value']
volume_headings = ['Date', 'Pork Loads', 'Trim/Process Loads']
cuts_headings = ['Date', 'Primal', 'Description', 'LBS', '$ Low', '$ High', '$ WgtAvg']

# Reference information to be included in the dataset descriptions for the benefit of
# quandl users
reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.'

#-------------------------Create the VOLUME dataset-------------------------------------------------------------
# Add relevant lines to a list and clean up        
volume_lines = url_lines[6:8] # Extract just the volume figures from the data set
volume_lines = [x.strip().split(':') for x in volume_lines] # Strip empty space padding and split lines in seperate fields

# volume_df holds the daily volume data
volume_df = pd.DataFrame(volume_lines) # Create a dataframe to store shipment volumes
volume_df = volume_df.T # Transpose the dataframe to make it a "long row" instead of a "deep column"
volume_df.insert(0, 'Date', report_dt, allow_duplicates = 'True') # Insert a date column before the other columns
volume_df = volume_df.drop(volume_df.index[0])
volume_df.columns = volume_headings # Assign correct column headings
volume_df = volume_df.set_index('Date') # Set Date field as the index


# Print quandl dataset for VOLUME
print 'code: USDA_LM_PK602_VOLUME'
print 'name: Daily pork volume (pork loads and trim/process loads)'
print 'description: Daily pork volume (pork loads and trim/process loads) from the USDA LM_PK602\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
print 'frequency: daily'
print 'private: false'
print '---'
volume_df.to_csv(sys.stdout)
print ''
print ''


#-------------------------Create the CUTOUT/PRIMAL dataset-------------------------------------------------------------
primal_cutout = url_lines[19:20][0].split() # Select the row containing the primal cutout values
del primal_cutout[1] # Drop the "Total Loads" value as it is not needed for this data set


# primal_df holds the daily cutout and primal data            
primal_df = pd.DataFrame(primal_cutout)
primal_df = primal_df.T
primal_df.columns = primal_headings
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
#---------------------------------------------------------------------------------------------

#-------------------------Create the PORK CUTS dataset----------------------------------------
cuts_df = pd.DataFrame(url_lines) # Put entire dataset into dataframe

# Get indices for start of each section
loin_start_index = cuts_df[cuts_df[0] == 'Loin\r\n'].index.tolist()[0].astype(int)
butt_start_index = cuts_df[cuts_df[0] == 'Butt\r\n'].index.tolist()[0].astype(int)
picnic_start_index = cuts_df[cuts_df[0] == 'Picnic\r\n'].index.tolist()[0].astype(int)
sparerib_start_index = cuts_df[cuts_df[0] == 'Sparerib\r\n'].index.tolist()[0].astype(int)
ham_start_index = cuts_df[cuts_df[0] == 'Ham\r\n'].index.tolist()[0].astype(int)
belly_start_index = cuts_df[cuts_df[0] == 'Belly\r\n'].index.tolist()[0].astype(int)
jowl_start_index = cuts_df[cuts_df[0] == 'Jowl\r\n'].index.tolist()[0].astype(int)
trim_start_index = cuts_df[cuts_df[0] == 'Trim\r\n'].index.tolist()[0].astype(int)
variety_start_index = cuts_df[cuts_df[0] == 'Variety\r\n'].index.tolist()[0].astype(int)
ai_start_index = cuts_df[cuts_df[0] == 'AI (Added Ingreds)\r\n'].index.tolist()[0].astype(int)

# Get indices for end of each section (just back up two lines from the start of the next section)
loin_end_index = butt_start_index - 2
butt_end_index = picnic_start_index - 2
picnic_end_index = sparerib_start_index - 2
sparerib_end_index = ham_start_index - 2
ham_end_index = belly_start_index - 2
belly_end_index = jowl_start_index - 2
jowl_end_index = trim_start_index - 2
trim_end_index = variety_start_index - 2
variety_end_index = ai_start_index - 2
ai_end_index = cuts_df[cuts_df[0] == 'Items that have no entries indicate there were trades but not reportable\r\n'] \
    .index.tolist()[0].astype(int) - 2

# From url_lines, extract the lines relevant to each pork cut in a separate list
loin_lines = url_lines[loin_start_index + 2:loin_end_index + 1]
butt_lines = url_lines[butt_start_index + 2:butt_end_index + 1]
picnic_lines = url_lines[picnic_start_index + 2:picnic_end_index + 1]
sparerib_lines = url_lines[sparerib_start_index + 2:sparerib_end_index + 1]
ham_lines = url_lines[ham_start_index + 2:ham_end_index + 1]
belly_lines = url_lines[belly_start_index + 2:belly_end_index + 1]
jowl_lines = url_lines[jowl_start_index + 2:jowl_end_index + 1]
trim_lines = url_lines[trim_start_index + 2:trim_end_index + 1]
variety_lines = url_lines[variety_start_index + 2:variety_end_index + 1]
ai_lines = url_lines[ai_start_index + 2:ai_end_index + 1]

all_lines = [loin_lines, butt_lines, picnic_lines, sparerib_lines, ham_lines, belly_lines, jowl_lines, \
    trim_lines, variety_lines, ai_lines]


#'''
#Iterate through the XML and extract the relevant data, placing it
#into python lists.  We are extracting three key items: the primal values
#for the day, the number of loads comprising the day's totals and the real
#"meat" of the report - the day's market values for the individual pork cuts.
#Each of these three items is placed in a separate list because the data
#structures are different for each.  These lists will later be converted
#to pandas dataframes for further manipulation prior to formatting as Quandl
#csv data.
#'''
#for report_date in root.report.iterchildren(): #processing must be repeated for each day covered by the report
#    date = report_date.attrib['report_date']
#    
#    for report in report_date.iterchildren(): #reports consist of the primals, loads and individual pork cuts
#        #print report.attrib['label']
#        if report.attrib['label'] == 'Cutout and Primal Values':
#            primal_cutout.append([date, report.record.attrib['pork_carcass'], report.record.attrib['pork_loin'], \
#            report.record.attrib['pork_butt'], report.record.attrib['pork_picnic'], report.record.attrib['pork_rib'], \
#            report.record.attrib['pork_ham'], report.record.attrib['pork_belly']])
#            
#        elif report.attrib['label'] == 'Current Volume':
#            loads.append([date, report.record.attrib['temp_cuts_total_load'], \
#            report.record.attrib['temp_process_total_load']])
#            
#        elif report.attrib['label'] in pork_cuts: # all individual cuts are handled with the same code
#            
#            for item in report.iterchildren():
#                
#                # Catch missing attributes (for cuts with no data for the day)
#                if 'total_pounds' not in item.attrib:
#                    item.attrib['total_pounds'] = '0'
#                    
#                if 'price_range_low' not in item.attrib:
#                    item.attrib['price_range_low'] = '0'
#                    
#                if 'price_range_high' not in item.attrib:
#                    item.attrib['price_range_high'] = '0'
#                    
#                if 'weighted_average' not in item.attrib:
#                    item.attrib['weighted_average'] = '0'
#                    
#                cuts.append([date, report.attrib['label'], \
#                item.attrib['Item_Description'], item.attrib['total_pounds'].replace(',', ''), \
#                item.attrib['price_range_low'], item.attrib['price_range_high'], \
#                item.attrib['weighted_average']])
#
# 
#
#cuts_df = pd.DataFrame(cuts, columns = cuts_headings)
#cuts_df.index = cuts_df['Date']
#

#

#
#
## Print quandl datasets for each pork cut
#'''
#for each pork cut in cuts_df:
#    filter cuts_df to only that specific cut
#    retain the cut in a variable
#    retain the primal in a variable
#    dynamically create a quandl code from the cut name
#    dynamically update the name from the primal value
#    dynamically update the description from the primal value and cut name
#    drop the date, primal and decription columns from cuts_df (leaving data index, lbs, low, high, avg)
#    print dynamically generated quandl metadata
#    print dataframe to stdout
#    print two blank lines in preparation for the next code
#    repeat for the next cut
#'''
#
## Compile regular expressions for use inside for loop below. Don't want to compile each time loop runs
## These regex objects are used later to ensure the format of the description complies with the
## requirements for a quandl csv file (upper case, alphanumeric)
#replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
#remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
#
#for cut in set(cuts_df['Description']): # Iterate through the list of unique Descriptions
#    fltrd_cuts_df = cuts_df[cuts_df['Description'] == cut]  # Subset the data to just a specific cut
#    primal = ''.join(set(fltrd_cuts_df['Primal']))
#    
#    # quandl code has to be uppercase, alphanumeric with no spaces. So need to convert to upper
#    # and remove any special characters using the regex module.  The specific regex objects were
#    # compiled above before code entered the FOR loop.
#    cut1 = replace.sub('_', cut) # replace certain characters with '_'
#    cut2 = remove.sub('', cut1).upper() # remove certain characters and convert to upper case
#    cut2 = cut2.translate(None, '-') # ensure '-' character is removed - don't know why regex didn't work...
#    quandl_code = 'USDA_LM_PK602_' + cut2 # build unique quandl code
#    
#    name = primal + ' - ' + cut
#    fltrd_cuts_df = fltrd_cuts_df.drop('Date', 1).drop('Primal', 1).drop('Description', 1)
#    
#    # Print quandl metadata
#    print 'code: ' + quandl_code
#    print 'name: Pork ' + name
#    print 'description: Daily total pounds, low price, high price and weighted average price ' \
#    '\n  from the USDA LM_PK602 report published by the USDA Agricultural Marketing Service ' \
#    '\n  (AMS). This dataset covers ' + name + '.\n' \
#     + reference_text
#    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
#    print 'frequency: daily'
#    print 'private: false'
#    print '---'
#    fltrd_cuts_df.to_csv(sys.stdout)
#    print ''
#    print ''
    
    
    

    
    
    
    
    
    
    