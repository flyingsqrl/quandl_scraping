# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary
"""

from lxml import etree, objectify
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
cuts = [] #"generic" list
primal_cutout = []
loads = []

# Iterate through the XML and extract the relevant data, placing it
# into python lists.
for report_date in root.report.iterchildren():
    date = report_date.attrib['report_date']
    
    for report in report_date.iterchildren():
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
            
                if report.attrib['label'] == 'loin_cuts':
                    loin_cuts = cuts[:]
                elif report.attrib['label'] == 'butt_cuts':
                    butt_cuts = cuts[:]
                elif report.attrib['label'] == 'picnic_cuts':
                    picnic_cuts = cuts[:]
                elif report.attrib['label'] == 'ham_cuts':
                    ham_cuts = cuts[:]
                elif report.attrib['label'] == 'belly_cuts':
                    belly_cuts = cuts[:]
                elif report.attrib['label'] == 'sparerib_cuts':
                    sparerib_cuts = cuts[:]
                elif report.attrib['label'] == 'jowl_cuts':
                    jowl_cuts = cuts[:]
                elif report.attrib['label'] == 'trim_cuts':
                    trim_cuts = cuts[:]
                elif report.attrib['label'] == 'variety_cuts':
                    variety_cuts = cuts[:]
                elif report.attrib['label'] == 'ai_cuts':
                    ai_cuts = cuts[:]
            

