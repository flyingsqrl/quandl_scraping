# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary
"""

from lxml import objectify
import pandas as pd

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
                item.attrib['Item_Description'], item.attrib['total_pounds'], \
                item.attrib['price_range_low'], item.attrib['price_range_high'], \
                item.attrib['weighted_average']])

'''
pandas import and manipulation
'''
primal_headings = ['Date', 'Carcass', 'Loin', 'Butt', 'Picnic', 'Rib', 'Ham', 'Belly']
volume_headings = ['Date', 'Total Loads', 'Trim/Process Loads']
cuts_headings = ['Date', 'Primal', 'Description', 'Total Pounds', 'Price - Low', 'Price - High', 'Wgt Avg Price']
            
primal_df = pd.DataFrame(primal_cutout, columns = primal_headings)
primal_df.index = primal_df['Date']
primal_df.drop(['Date'],inplace=True,axis=1) 

volume_df = pd.DataFrame(loads, columns = volume_headings)
volume_df.index = volume_df['Date']
volume_df.drop(['Date'],inplace=True,axis=1) 

cuts_df = pd.DataFrame(cuts, columns = cuts_headings)
cuts_df.index = cuts_df['Date']
cuts_df.drop(['Date'],inplace=True,axis=1) 

loin_df = cuts_df[cuts_df['Primal'] == 'Loin Cuts']