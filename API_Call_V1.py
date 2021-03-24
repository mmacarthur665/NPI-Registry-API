"""
Program Name: Calling NPI Registry API
Author: Michael Macarthur
Date: 3/17/2021

Purpose: Call NPI Registry API and pull out all Nephrologists - idea is to try to identify new ones
"""

import requests
import json
from flatten_json import flatten
import pandas as pd
import itertools
import glob
import os
import time

pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_columns", 50)
timestr = time.strftime("%m%d%Y") # creates datetime string so date can be put in filename
#-----------------------------------------------------------------------------------------------------------------------
#PART 1 - Pulling data from npi registry api
# skip and states are data structures which will be manipulated into tuples which we can then insert easily into the
# urls when the loop begins

skip = ['','200','400','600','800','1000'] # this list is for the skip parameter of the api
states = ['AE', 'AK', 'AL', 'AP', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'FM', 'GA', 'GU', 'HI',
          'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC',
          'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
          'VA', 'VI', 'VT', 'WA', 'WI', 'WV']

##The states list is the info that will be inserted into the state parameter of the api

states = list(itertools.chain.from_iterable(itertools.repeat(x,6) for x in states)) # repeats every item of state list 6 times
skip = skip*354 # repeats skip list 360 times (this sets up the tuple creation)

vars = tuple(zip(states, skip)) # converts states & skip into a list of tuples which we will insert into our url string

f_cnt = 0 # counts number of times loop ran
api_data = []
for v in vars:
    url = 'https://npiregistry.cms.hhs.gov/api/?number=&enumeration_type=NPI-1&taxonomy_description=neph*&first_name=&use_first_name_alias=&last_name=&organization_name=&address_purpose=&city=&state='+v[0]+'&'+'postal_code=&country_code=US&limit=200&skip='+v[1]+'&pretty=on&version=2.1'
    r = requests.get(url) # opens connection to engineered url
    results_json = r.json() # reads api output into a json object
    result_count = results_json['result_count'] # counts entries in npi reg meeting our crteria
    i = 0 # sets counter
    #Create loop
    while i < result_count:
        record = flatten(results_json['results'][i]) # flattens json object into dict
        data = pd.DataFrame.from_dict(record,orient='index').T # stores text from each record in dict in dataframe object
        if i == 0:  # if first iteration of loop - all is written to dataframe
            df = data
        else:
            df = pd.concat([df,data], axis=0, ignore_index=True) # if not first iteration of loop, then add new row to dataframe
        i += 1 # increases while loop counter
    r.close() # closes connection to api
    f_cnt+=1 # increases file name counter
    api_data.append(df)

output = pd.concat(api_data)
deduped = output.drop_duplicates(subset=['number'], keep='first', ignore_index=True)
