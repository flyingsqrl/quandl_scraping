# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary
"""

from lxml import objectify
import pandas as pd
import sys
import re
import requests

url = 'http://mpr.datamart.ams.usda.gov/ws/report/v1/pork/LM_PK602?filter={"filters":[{"fieldName":"Report Date","operatorType":"GREATER","values":["03/31/2014"]}]}'

r = requests.get(url)
xml = r.text.encode('ascii', 'ignore') # Convert requests.get() response from unicode to ascii in prep for lxml parsing
xml = xml.replace('null', '0') # Replace null quantities with zeros
    
root = objectify.fromstring(xml) # Prepare the XML tree for parsing

# List of categories of pork cuts for use later in routine
pork_cuts= ['Loin Cuts', 'Butt Cuts', 'Picnic Cuts', 'Ham Cuts', 'Belly Cuts', \
    'Sparerib Cuts', 'Jowl Cuts', 'Trim Cuts', 'Variety Cuts', \
    'Added Ingredient Cuts']

# Initialize empty python lists to hold data to be extracted from the XML
cuts = [] #"generic" list to store the USDA market data for individual cuts
primal_cutout = [] # list to store primal cutout values
loads = [] # list to store the number of loads comprising the market for the day

'''
Iterate through the XML and extract the relevant data, placing it
into python lists.  We are extracting three key items: the primal values
for the day, the number of loads comprising the day's totals and the real
"meat" of the report - the day's market values for the individual pork cuts.
Each of these three items is placed in a separate list because the data
structures are different for each.  These lists will later be converted
to pandas dataframes for further manipulation prior to formatting as Quandl
csv data.
'''
for report_date in root.report.iterchildren(): #processing must be repeated for each day covered by the report
    date = report_date.attrib['report_date']
    
    for report in report_date.iterchildren(): #reports consist of the primals, loads and individual pork cuts
        #print report.attrib['label']
        if report.attrib['label'] == 'Cutout and Primal Values':
            primal_cutout.append([date, report.record.attrib['pork_carcass'], report.record.attrib['pork_loin'], \
            report.record.attrib['pork_butt'], report.record.attrib['pork_picnic'], report.record.attrib['pork_rib'], \
            report.record.attrib['pork_ham'], report.record.attrib['pork_belly']])
            
        elif report.attrib['label'] == 'Current Volume':
            loads.append([date, report.record.attrib['temp_cuts_total_load'], \
            report.record.attrib['temp_process_total_load']])
            
        elif report.attrib['label'] in pork_cuts: # all individual cuts are handled with the same code
            
            for item in report.iterchildren():
                
                # Catch missing attributes (for cuts with no data for the day)
                if 'total_pounds' not in item.attrib:
                    item.attrib['total_pounds'] = '0'
                    
                if 'price_range_low' not in item.attrib:
                    item.attrib['price_range_low'] = '0'
                    
                if 'price_range_high' not in item.attrib:
                    item.attrib['price_range_high'] = '0'
                    
                if 'weighted_average' not in item.attrib:
                    item.attrib['weighted_average'] = '0'
                    
                cuts.append([date, report.attrib['label'], \
                item.attrib['Item_Description'], item.attrib['total_pounds'].replace(',', ''), \
                item.attrib['price_range_low'], item.attrib['price_range_high'], \
                item.attrib['weighted_average']])

'''
Each of the three sections of the report (primal/cutout, volume, indivudual pork cuts)
are placed into pandas dataframes to allow easy manipulation into the format required of
a quandl csv file.
'''
primal_headings = ['Date', 'Carcass Value', 'Loin Value', 'Butt Value', 'Picnic Value', 'Rib Value', 'Ham Value', 'Belly Value']
volume_headings = ['Date', 'Total Loads', 'Trim/Process Loads']
cuts_headings = ['Date', 'Primal', 'Description', 'LBS', '$ Low', '$ High', '$ WgtAvg']

# primal_df holds the daily cutout and primal data            
primal_df = pd.DataFrame(primal_cutout, columns = primal_headings)
primal_df.index = primal_df['Date']
primal_df.drop(['Date'],inplace=True,axis=1) 

# volume_df holds the daily shipment volume information
volume_df = pd.DataFrame(loads, columns = volume_headings)
volume_df.index = volume_df['Date']
volume_df.drop(['Date'],inplace=True,axis=1) 

cuts_df = pd.DataFrame(cuts, columns = cuts_headings)
cuts_df.index = cuts_df['Date']

#----------------------------------------------------------------------------------------------
# Print quandl dataset for CUTOUT AND PRIMAL VALUES
print 'code: USDA_LM_PK602_CUTOUT_PRIMAL'
print 'name: Daily USDA pork cutout and primal values'
print 'description: |'
print 'Daily pork cutout and primal values from the USDA LM_PK602 report published by the USDA'
print 'Agricultural Marketing Service (AMS).'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
print 'frequency: daily'
print 'private: true'
print '---'
primal_df.to_csv(sys.stdout)
print ''
print ''
#---------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------
# Print quandl dataset for VOLUME
print 'code: USDA_LM_PK602_VOLUME'
print 'name: Daily pork volume (full loads and trim/process loads)'
print 'description: |'
print 'Daily pork volume (full loads and trim/process loads) from the USDA LM_PK602 report'
print 'published by the USDA Agricultural Marketing Service (AMS).'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
print 'frequency: daily'
print 'private: true'
print '---'
volume_df.to_csv(sys.stdout)
print ''
print ''
#---------------------------------------------------------------------------------------------


# Print quandl datasets for each pork cut
'''
for each pork cut in cuts_df:
    filter cuts_df to only that specific cut
    retain the cut in a variable
    retain the primal in a variable
    dynamically create a quandl code from the cut name
    dynamically update the name from the primal value
    dynamically update the description from the primal value and cut name
    drop the date, primal and decription columns from cuts_df (leaving data index, lbs, low, high, avg)
    print dynamically generated quandl metadata
    print dataframe to stdout
    print two blank lines in preparation for the next code
    repeat for the next cut
'''

# Compile regular expressions for use inside for loop below. Don't want to compile each time loop runs
# These regex objects are used later to ensure the format of the description complies with the
# requirements for a quandl csv file (upper case, alphanumeric)
replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
remove = re.compile('[,%#]') # list of characters to be removed from the pork cut description

for cut in set(cuts_df['Description']): # Iterate through the list of unique Descriptions
    fltrd_cuts_df = cuts_df[cuts_df['Description'] == cut]  # Subset the data to just a specific cut
    primal = ''.join(set(fltrd_cuts_df['Primal']))
    
    # quandl code has to be uppercase, alphanumeric with no spaces. So need to convert to upper
    # and remove any special characters using the regex module.  The specific regex objects were
    # compiled above before code entered the FOR loop.
    cut1 = replace.sub('_', cut) # replace certain characters with '_'
    cut2 = remove.sub('', cut1).upper() # remove certain characters
    quandl_code = 'USDA_LM_PK602_' + cut2
    
    name = primal + ' - ' + cut
    fltrd_cuts_df = fltrd_cuts_df.drop('Date', 1).drop('Primal', 1).drop('Description', 1)
    
    # Print quandl metadata
    print 'code: ' + quandl_code
    print 'name: Pork ' + name
    print 'description: |'
    print 'Daily total pounds, low price, high price and weighted average price'
    print 'from the USDA LM_PK602 report published by the USDA Agricultural Marketing Service (AMS).'
    print 'This dataset covers ' + name + '.'
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
    print 'frequency: daily'
    print 'private: true'
    print '---'
    fltrd_cuts_df.to_csv(sys.stdout)
    print ''
    print ''
    
    
    

    
    
    
    
    
    
    