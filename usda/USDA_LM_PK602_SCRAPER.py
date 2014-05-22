# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary
"""

from lxml import objectify
import pandas as pd
import sys

file_name = './data/LM_PK602_sample_lmr_ws.xml'

with open(file_name) as f:
    xml = f.read()
    
root = objectify.fromstring(xml)

desired_aggregate_figures = ['Cutout and Primal Values', 'Current Volume']
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
pandas import and manipulation
'''
primal_headings = ['Date', 'Carcass Value', 'Loin Value', 'Butt Value', 'Picnic Value', 'Rib Value', 'Ham Value', 'Belly Value']
volume_headings = ['Date', 'Total Loads', 'Trim/Process Loads']
cuts_headings = ['Date', 'Primal', 'Description', 'LBS', '$ Low', '$ High', '$ WgtAvg']
            
primal_df = pd.DataFrame(primal_cutout, columns = primal_headings)
primal_df.index = primal_df['Date']
primal_df.drop(['Date'],inplace=True,axis=1) 

volume_df = pd.DataFrame(loads, columns = volume_headings)
volume_df.index = volume_df['Date']
volume_df.drop(['Date'],inplace=True,axis=1) 

cuts_df = pd.DataFrame(cuts, columns = cuts_headings)
cuts_df.index = cuts_df['Date']
#cuts_df.drop(['Date'],inplace=True,axis=1) 

loin_df = cuts_df[cuts_df['Primal'] == 'Loin Cuts'] # filter everything but loins
butt_df = cuts_df[cuts_df['Primal'] == 'Butt Cuts'] # filter everything but loins
picnic_df = cuts_df[cuts_df['Primal'] == 'Picnic Cuts'] # filter everything but loins
ham_df = cuts_df[cuts_df['Primal'] == 'Ham Cuts'] # filter everything but loins
belly_df = cuts_df[cuts_df['Primal'] == 'Belly Cuts'] # filter everything but loins
sparerib_df = cuts_df[cuts_df['Primal'] == 'Sparerib Cuts'] # filter everything but loins
jowl_df = cuts_df[cuts_df['Primal'] == 'Jowl Cuts'] # filter everything but loins
trim_df = cuts_df[cuts_df['Primal'] == 'Trim Cuts'] # filter everything but loins
variety_df = cuts_df[cuts_df['Primal'] == 'Variety Cuts'] # filter everything but loins
ai_df = cuts_df[cuts_df['Primal'] == 'Added Ingredient Cuts'] # filter everything but loins


'''
Format the dataframes to comply with Quandl csv format, write Quandl metadata to stdout and
then write the dataframe to stdout.  This is done for each Primal dataframe in turn.

To format the dataframe, we first "melt" it to get the data into "long" format,
with only a single column containing numeric values, and another column that indicates
the quantity represented by the value (pounds or prices).  We then create a new column
"Attribute" consisting of a concatenation of the cut (ex "1/4 Trimmed Loin Paper")
and the measure (ex LBS or $ High).  This facilitates later formatting into Quandl csv
format.  We then drop the "Primal" and Description columns from the dataframe.  Finally,
we pivot the dataframe back into into "wide" format, with only one row for each data, and many
columns to store the different cuts and their numeric measures.  (The "Attribute" column
created earlier.)
'''

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
## format dataframe
#loin_df = pd.melt(loin_df, id_vars = ['Date', 'Primal', 'Description'])
#loin_df['Attribute'] = loin_df['Description'] + '::' + loin_df['variable']
#loin_df = loin_df.drop('Primal', 1).drop('Description', 1)
#loin_df = loin_df.pivot(index='Date', columns='Attribute', values='value')
#
#
## Print quandl CSV metadata
#print 'code: USDA_LM_PK602_LOIN'
#print 'name: Loin data from the USDA LM_PK602 daily report'
#print 'description: |'
#print 'Daily total pounds, low price, high price and weighted average price'
#print 'from the USDA LM_PK602 report published by the USDA Agricultural Marketing Service (AMS).'
#print 'The data is published in "wide" format, allowing each specific loin cut'
#print 'to be listed.'
#print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
#print 'frequency: daily'
#print 'private: true'
#print '---'
#
## Print the actual data to stdout
#loin_df.to_csv(sys.stdout)

    
    

    
    
    
    
    
    
    