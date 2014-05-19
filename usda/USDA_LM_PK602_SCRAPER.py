# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary
"""

from lxml import etree, objectify

file_name = './data/LM_PK602_sample_lmr_ws.xml'

with open(file_name) as f:
    xml = f.read()
    
root = objectify.fromstring(xml)

print root.attrib

for report_date in root.report.iterchildren():
    print report_date.attrib['report_date']


desired_aggregate_figures = ['Cutout and Primal Values', 'Current Volume']
pork_cuts= ['Loin Cuts', 'Butt Cuts', 'Picnic Cuts', 'Ham Cuts', 'Belly Cuts', \
    'Sparerib Cuts', 'Jowl Cuts', 'Trim Cuts', 'Variety Cuts', \
    'Added Ingredient Cuts']

primal_cutout = []
loads = []
loin_cuts = []
butt_cuts = []
picnic_cuts = []
ham_cuts = []
belly_cuts = []
sparerib_cuts = []
jowl_cuts = []
trim_cuts = []
variety_cuts = []
ai_cuts = []


for report_date in root.report.iterchildren():
    date = report_date.attrib['report_date']
    print date
    for report in report_date.iterchildren():
        print report.attrib['label']
        if report.attrib['label'] == 'Cutout and Primal Values':
            primal_cutout.append([date, report.record.attrib['pork_carcass'], report.record.attrib['pork_loin'], \
            report.record.attrib['pork_butt'], report.record.attrib['pork_picnic'], report.record.attrib['pork_rib'], \
            report.record.attrib['pork_ham'], report.record.attrib['pork_belly']])
        elif report.attrib['label'] == 'Current Volume':
            loads.append([date, report.record.attrib['temp_cuts_total_load'], \
            report.record.attrib['temp_process_total_load']])
        elif report.attrib['label'] in pork_cuts:
            
