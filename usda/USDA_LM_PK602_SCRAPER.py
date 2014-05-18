# -*- coding: utf-8 -*-
"""
Created on Sun May 18 00:04:55 2014

@author: billcary
"""

from lxml import etree

file_name = './data/LM_PK602_sample_lmr_ws.xml'
root = etree.parse(file_name)

report_dates = root.list()[0].list()

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


for date in report_dates:
    for report in date:
        values = report.list()
        if report.attrib['label'] == 'Cutout and Primal Values':
            primal_cutout.append(date, values.attrib('pork_carcass'), values.attrib('pork_loin'), \
            values.attrib('pork_loin'), values.attrib('pork_picnic'), values.attrib('pork_rib'), \
            values.attrib('pork_ham'), values.attrib('pork_belly'))
