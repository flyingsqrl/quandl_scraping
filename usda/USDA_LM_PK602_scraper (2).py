"""
Created on Fri May 30 08:00:53 2014

@author: nataliecmoore

Script Name: USDA_LM_PK602_SCRAPER

Purpose:
Retrieve daily USDA pork data from from the LM_PK602 report via the USDA LMR
web service for upload to Quandl.com. The script pulls data for
volume, cutout/primal values and data for each individual cut of pork tracked by
USDA.

Approach:
Used python string parsing to find weight, min cost, max cost, and weighted average
for each pork cut. Made a csv table for each cut and formatted to upload to Quandl.

Author: Natalie Moore

History:

Date            Author          Purpose
-----           -------         -----------
05/30/2014      Natalie Moore   Initial development/release

"""

import datetime 
import urllib2
import pandas as pd
import sys
import re
import pytz

date=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d") #holds the date in YYYY-MM-DD format
# store report in variable
url="http://www.ams.usda.gov/mnreports/lm_pk602.txt"
site_contents=urllib2.urlopen(url).read()

# initialize lists that will later hold values for each cut
cuts=['Loin', 'Butt', 'Picnic', 'Sparerib', 'Ham', 'Belly', 'Jowl','Trim', 'Variety', 'AI'] 
primal_cutout=['Date','Carcass Value', 'Loin Value', 'Butt Value', 'Picnic Value', 'Rib Value', 'Ham Value', 'Belly Value'] 
cuts_sublist=[]    #holds names of each cut
weight=[]          #holds weight of each cut (in lbs)
min_cost=[]        #holds minimum cost of each cut (in cents)
max_cost=[]        #holds maximum cost of each cut (in cents)
wtd_avg=[]         #holds weighted average of each cut's cost
primal=[]          #holds primal cut associated with each cut

#Finds the daily pork volume (full loads and trim/process loads)
d_tl_pork=site_contents.find(':', site_contents.find("Loads PORK CUTS"))
d_tl_pork=site_contents[d_tl_pork+6:site_contents.find("\r\n", d_tl_pork+6)]
d_tp_pork=site_contents.find(':', site_contents.find("Loads TRIM/PROCESS PORK"))
d_tp_pork=site_contents[d_tp_pork+6:site_contents.find("\r\n", d_tp_pork+6)]


#Finds the daily pork cutout and primal values
primal_cutout_list=[]
starting_index=site_contents.find("\r\n", site_contents.find("\r\n", site_contents.find("Belly"))+1)
ending_index=site_contents.find("\r\n", starting_index+1)
row=site_contents[starting_index+17:ending_index]
primal_row=re.compile("\s\s+").split(row)

# Creates a list where each entry is a row of data on the website. Each of these entries is a list
# where every entry is a different attribute associated with each cut. 
# Ex: 1/4 Trimmed Loin VAC                      455,569   134.41 - 159.00   140.35
#     becomes: [' 1/4 Trimmed Loin VAC', '455,569', '134.41 - 159.00', '140.35']
line_list=[]       
lines= re.compile("\r?\n").split(site_contents)
pattern=re.compile("\s\s+")
for line in lines:
    newline=pattern.split(line)
    line_list.append(newline)
    
# Filters the list by removing:
# 1. Lists that have no data
# 2. Lists that have no data as its first entry but have data following
# 3. Lists that don't begin with a space or hyphen (removes unnecessary entries from the beginning and end)     
line_list=[line_list[x] for x in range(len(line_list)) if len(line_list[x])!=0]
line_list=[line_list[x] for x in range(len(line_list)) if line_list[x][0]!=""]
line_list=[line_list[x] for x in range(len(line_list)) if (line_list[x][0][0]=="-" or line_list[x][0][0]==" ")]

# Deletes hyphenated entries that don't mark the border between primal cuts.
# The hyphenated entries that remain are later used to fill in the primal list so that
# each cut can be associated with a primal cut
x=0
while x!=1:
   if line_list[x][0][0]!='-':
       x=1
   else:
       del line_list[x]
       
       
# Loops through each list in line_list and stores the cut, weight, minimum cost,
# maximum cost, and weighted average into their corresponding list. 
# To find the primal associated with each cut, primal_index increments every time a 
# hyphenated line is found. The list "cuts" is then indexed using this value so that
# the primal at that index is stored in the primal list.
x=0
primal_index=0
while x<len(line_list):
    if line_list[x][0][0]=='-':
        del line_list[x]                          #delete hyphenated line from list
        primal_index=primal_index+1               #increment primal_index because next primal is needed
    elif len(line_list[x])==1:                    
        del line_list[x]                          #delete the unnecessary lines
    else:
        primal.append(cuts[primal_index])         #add the corresponding primal to the primal list
        cuts_sublist.append(line_list[x][0])      #add the corresponding cut to the cuts list
        # These next two if statements are used to correct the line splitting error that occurs
        # if the weight is 1,000,000 or above. This separates the min and max cost from being in the 
        # same entry as the weight
        if line_list[x][1].find(' ')==-1:        
            weight.append(line_list[x][1])        
        if line_list[x][1].find(' ')!=-1:
            s_index=line_list[x][1].find(' ')
            weight.append(line_list[x][1][0:s_index])
            line_list[x].insert(2, line_list[x][1][s_index:])
        if len(line_list[x])!=2:                  #if the list doesn't contain empty values
            # The following if/else statements are used to correct an error that sometimes occurs
            # when the lines are being split into rows. Occasionally, the min and max values are in 
            # different entries in the list instead of being included together in the same entry.
            dash_index=line_list[x][2].find("-")
            if dash_index==-1:
                min_cost.append(line_list[x][2])
                max_cost.append(line_list[x][3][3:])
                wtd_avg.append(line_list[x][4])
            else:            
                min_cost.append(line_list[x][2][:dash_index])
                max_cost.append(line_list[x][2][dash_index+1:])
                wtd_avg.append(line_list[x][3])
        else:                                      #if the rows are empty, add 0 for each category
            min_cost.append(0)
            max_cost.append(0)
            wtd_avg.append(0)
        x=x+1

# Changes each entry in weight by removing commas, converting empty values to 0,
# and changing each entry to an integer        
n=0
while n<len(weight):
    weight[n]=weight[n].replace(',', '')
    weight= [x if x!='-' else '0' for x in weight] #change each empty entry to 0
    n=n+1
weight=[int(i) for i in weight]

# Changes each entry in lists "min_cost" "max_cost" and "wtd_avg" from a string
# to a floating point 
s_list=[min_cost, max_cost, wtd_avg]
s=0
while s <3:
    s_list[s]=[float(i) for i in s_list[s]]
    s=s+1
min_cost=s_list[0]
max_cost=s_list[1]
wtd_avg=s_list[2]


# volume_df holds the daily shipment volume information
volume_headings = ['Date', 'Total Loads', 'Trim/Process Loads']
loads={'Date': [date], 'Total Loads': [d_tl_pork], 'Trim/Process Loads': [d_tp_pork]}
volume_df = pd.DataFrame(loads, columns = volume_headings)
volume_df.index = volume_df['Date']
volume_df.drop(['Date'],inplace=True,axis=1) 


reference_text = '  Historical figures from USDA can be verified using the LMR datamart located ' \
    '\n  at http://mpr.datamart.ams.usda.gov.\n' \
    '  All pricing is on a per CWT (100 lbs) basis.'
    
# Print quandl dataset for VOLUME
print 'code: USDA_LM_PK602_VOLUME\n'
print 'name: Daily pork volume (full loads and trim/process loads)\n'
print 'description: "Daily pork volume (full loads and trim/process loads) from the USDA LM_PK602\n' \
    '  report published by the USDA Agricultural Marketing Service (AMS).\n' \
    + reference_text + '"\n'
print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt\n'
print 'frequency: daily\n'
print 'private: false\n'
print '---\n'
volume_df.to_csv(sys.stdout)
print '\n'
print '\n'

# primal_df holds the daily cutout and primal data  
primal_loads={'Date': [date], 'Carcass Value': [primal_row[1]], 'Loin Value': [primal_row[2]], 'Butt Value': [primal_row[3]], 'Picnic Value': [primal_row[4]], 'Rib Value': [primal_row[5]],\
 'Ham Value': [primal_row[6]], 'Belly Value': [primal_row[7]]}
primal_df = pd.DataFrame(primal_loads, columns=primal_cutout)
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

# Loops through each cut and creates a csv table that is formatted to upload to Quandl.
s=0
while s<len(cuts_sublist):
    cuts_headings = [ 'Date', 'Primal', 'LBS', '$ Low', '$ High', '$ WgtAvg']
    data={'Date': [date], 'Primal': [primal[s]], 'LBS': [weight[s]], '$ Low': [min_cost[s]], '$ High': [max_cost[s]], '$ WgtAvg': [wtd_avg[s]]}
    cuts_df = pd.DataFrame(data, columns = cuts_headings)
    cuts_df.index = cuts_df['Date']
    cuts_df = cuts_df.drop('Date', 1).drop('Primal', 1)
    replace = re.compile('[ /]') # list of characters to be replaced in the pork cut description
    remove = re.compile('[,%#-&()!$+<>?/\'"{}.*@]') # list of characters to be removed from the pork cut description
    cut1 = replace.sub('_', cuts_sublist[s]) # replace certain characters with '_'
    cut2 = remove.sub('', cut1).upper() # remove certain characters and convert to upper case
    cut2 = cut2.translate(None, '-') # ensure '-' character is removed
    quandl_code = 'USDA_LM_PK602_' + cut2 # build unique quandl code
    print 'code: ' + quandl_code
    print 'name: Pork ' +primal[s]+"Cuts"+' - '+cuts_sublist[s]
    print 'description: Daily total pounds, low price, high price and weighted average price ' \
    '\n  from the USDA LM_PK602 report published by the USDA Agricultural Marketing Service ' \
    '\n  (AMS). This dataset covers ' + cuts_sublist[s] + '.\n'\
    + reference_text
    print 'reference_url: http://www.ams.usda.gov/mnreports/lm_pk602.txt'
    print 'frequency: daily'
    print 'private: false'
    print '---'
    cuts_df.to_csv(sys.stdout)
    print ''
    print ''
    s=s+1
















