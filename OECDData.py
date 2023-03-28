# -*- coding: utf-8 -*-
"""
Created on Wed Mar 2 12:29:41 2022

Functions to download data from OECD. Please consult documentation of individual functions below for further information.

@author: Lars E. Spreng
"""
import pandas as pd 
from lxml import etree
import requests as rq
from functools import reduce
import xmltodict
from datetime import datetime

def get_var_codes_MEIArchive():
    url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/MEI_ARCHIVE"
    resp = rq.get(url)
    doc = etree.fromstring(resp.content)
    
    root ="{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/*[@id='CL_MEI_ARCHIVE_VAR']/"
    var_list = doc.findall(root)
    var_code = [int(var_list[i].get('value')) for i in range(2,len(var_list))]
    
    root = "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}Description"
    
    var_description = [var_list[i].findall(root)[0].text for i in range(2,len(var_list))]
    return var_code, var_description

def get_country_codes_MEIArchive():
    url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/MEI_ARCHIVE"
    resp = rq.get(url)
    doc = etree.fromstring(resp.content)
    
    root ="{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/*[@id='CL_MEI_ARCHIVE_LOCATION']/"
    country_list = doc.findall(root)
    country_code = [country_list[i].get('value') for i in range(2,len(country_list))]

    return country_code

def get_series_first_release_MEIArchive(country_list, variable_list, frequency,  startDate, endDate, startEDI, endEDI):     
    # Request data from OECD API and return pandas DataFrame
    
    # =============== INPUT 
    # country_list: list of countries
    # variable_list: list of variabes
    # frequency: 'M' for monthly and 'Q' for quarterly time series
    # startDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # endDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # startEDI: Edition of data, i.e. when it was published in YYYYMM format
    # endEDI: Final edition in YYYYMM format
    
    # =============== RAW DATA STRUCTURE
    # The dataset has a total of M series which are identified through four keys in the following format: 0:0:0:0
    # Position 1: Country
    # Position 2: Variable
    # Position 3: Edition of Data
    # Position 4: Frequency
    # Each series contains n observations for each time period, identified through a number t
    # For example, for country "GBR", variable "201" with frequency M, between 1999-01 to 1999-12 the series for
    # edition 202201 contains 12 observations. The series for edition 1999-03 will contain maximum 3 observations.
    # it is possible that t is not a consecutive series of values in which case observations are missing. 
    # Code accounts for differences in length of time series.
    # Real time data is extracted as the observations in the first published edition.
    
    # ============= Create URL
    url_base = "https://stats.oecd.org/sdmx-json/data/MEI_ARCHIVE/"
    
    if isinstance(variable_list,list) == True:
        if len(variable_list) == 1:
            variable_str = str(variable_list[0])
        else:
            variable_str = '+'.join(str(x) for x in variable_list)
    else:
        variable_str = str(variable_list)
        
    if isinstance(country_list,list) == True:
        N = len(country_list)
        if len(country_list) == 1:
            country_str = country_list[0]
        else:
            country_str = '+'.join(str(x) for x in country_list)
    else:
        N = 1;
        country_str = country_list
    
    startTime = "startTime=" + startDate
    endTime = "endTime=" + endDate

    if startEDI == [] and endEDI == []:
        if float(startDate.replace('-','')) >= 199902:
            edition_dates = pd.date_range(datetime.strptime(startDate, '%Y-%m'),datetime.now(),freq='m').strftime('%Y%m')
        else:
            edition_dates = pd.date_range(datetime.strptime('1999-02', '%Y-%m'),datetime.now(),freq='m').strftime('%Y%m')

    elif endEDI == []:
        edition_dates = pd.date_range(datetime.strptime(startEDI, '%Y-%m'),datetime.now(),freq='m').strftime('%Y%m')
    elif startEDI == []:
        edition_dates = pd.date_range(datetime.strptime(startDate, '%Y-%m'),datetime.strptime(endEDI, '%Y-%m'),freq='m').strftime('%Y%m')
    else:
        edition_dates = pd.date_range(datetime.strptime(startEDI, '%Y-%m'),datetime.strptime(endEDI, '%Y-%m'),freq='m').strftime('%Y%m')

    edition_str = '+'.join(str(x) for x in edition_dates)
    
    url = url_base + country_str + "." + variable_str + "." + edition_str + "." + frequency + "/all?" + startTime + "&" + endTime 
  
    # ============= Download Data
    response = rq.get(url = url, params = {})

    if (response.status_code == 200):
        
        responseJson = response.json()
        # Get list of observations. This includes all revision to variables, not just real time vintages
        series = responseJson.get('dataSets')[0].get('series')
        filterKeys = lambda k: {x: series[x] for x in k}

        if (len(series) > 0):
            
            # Countries in dataset
            temp = responseJson.get('structure').get('dimensions').get('series')[0].get('values') 
            countries = [temp[i].get('id') for i in range(len(temp))]
            
            # All available time periods. Does NOT necessarily equal all time periods per country
            temp = responseJson.get('structure').get('dimensions').get('observation')[0].get('values')
            dates = [temp[i].get('id') for i in range(len(temp))]
            # Remove wrong frequency (sometimes in there by accident)
            if  frequency == 'M':
                dates = [item for item in dates if "Q" not in item]
            elif frequency == 'Q':
                dates = [item for item in dates if "M" not in item]
            # All editions in dataset
            temp = responseJson.get('structure').get('dimensions').get('series')[2].get('values')
            editions = [temp[i].get('id') for i in range(len(temp))]
            
            # Units of variables
            units = responseJson.get('structure').get('attributes').get('series')[1].get('values')

            if N == 1:
                temp = list(series.values())      
                tempObs = [temp[i].get('observations') for i in range(len(temp))]  
                tempKeys = [list(tempObs[i].keys()) for i in range(len(tempObs))]
                
                realObs = []
                for i in range(len(tempObs)):
                    if i == 0:
                        realObs.extend([tempObs[0][j][0] for j in set(tempKeys[0])])
                    else:
                        newKey = set(tempKeys[i]) - set().union(*tempKeys[0:i]);
                        if len(newKey) > 0:
                            realObs.extend([tempObs[i][j][0] for j in newKey])
                    
                df = pd.DataFrame(dates)
                df[countries[0]] = realObs
                return df

            elif len(countries) > 1:
            
                # Get all keys
                key = list(series.keys())       
                key_id = [key[i].split(':')[0] for i in range(len(key))]
                # Create empty dataframe with all dates as index
                df = pd.DataFrame(dates)
                df.set_index(0, inplace=True)
                
                for j in range(len(countries)):
                    # Get series per country
                    splitKeys = [key[i] for i in range(len(key)) if key_id[i]==str(j)]
                    tempseries = filterKeys(splitKeys)
                    temp = list(tempseries.values())   
                    
                    if len(temp) == 0:
                        
                        print('Error: No results for requested variable no.' + variable_str + 'for country' + countries[j])
                    
                    else: 
                        # All observations for each edition
                        tempObs = [temp[i].get('observations') for i in range(len(temp))]  
                        # Keys (t) to identify time periods
                        tempKeys = [list(tempObs[i].keys()) for i in range(len(tempObs))]
                        # Get real time data
                        realObs = []
                        for i in range(len(tempObs)):
                            if i == 0:
                                # All observations for first edition
                                realObs.extend([tempObs[0][k][0] for k in set(tempKeys[0])])
                            else:
                                # Get keys for observations for time periods that have not been published before 
                                # (i.e. have been revised)
                                newKey = set(tempKeys[i]) - set().union(*tempKeys[0:i]);
                                if len(newKey) > 0:
                                # If current edition includes new observations, add to real time data
                                    realObs.extend([tempObs[i][k][0] for k in newKey])
                        # Get keys to identify dates corresponding to real time observations
                        allKeys = [int(item) for item in list(set().union(*tempKeys))]
                        allKeys.sort()
                        # Get dates corresponding to real time observations
                        tempDates = [dates[i] for i in allKeys]
                        # Create dataframe for country j
                        df_temp = pd.DataFrame(tempDates)
                        df_temp.set_index(0, inplace=True)
                        df_temp[countries[j]] = realObs
                        # Combine with countries from previous iteration, fill missing dates with nan
                        df = df.join(df_temp)
                    
                return df, units

        else:
            
            print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)
            
    elif (response.status_code == 404):
        
          print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)

    else:

        print('Error: %s' % response.status_code)
        print('Error: Check URL. Made request from: /r/n' + url)

def get_series_all_releases_MEIArchive(country_list, variable_list, frequency,  startDate, endDate, startEDI, endEDI):     
    # Request data from OECD API and return pandas DataFrame
    
    # =============== INPUT 
    # country_list: list of countries
    # variable_list: list of variabes
    # frequency: 'M' for monthly and 'Q' for quarterly time series
    # startDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # endDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # startEDI: Edition of data, i.e. when it was published in YYYYMM format
    # endEDI: Final edition in YYYYMM format
    
    # =============== RAW DATA STRUCTURE
    # The dataset has a total of M series which are identified through four keys in the following format: 0:0:0:0
    # Position 1: Country
    # Position 2: Variable
    # Position 3: Edition of Data
    # Position 4: Frequency
    # Each series contains n observations for each time period, identified through a number t
    # For example, for country "GBR", variable "201" with frequency M, between 1999-01 to 1999-12 the series for
    # edition 202201 contains 12 observations. The series for edition 1999-03 will contain maximum 3 observations.
    # it is possible that t is not be a consecutive series of values in which case observations are missing. 
    # Code accounts for differences in length of time series.
    # Real time data is extracted as the observations in the first published edition.
    
    # ============= Create URL
    url_base = "https://stats.oecd.org/sdmx-json/data/MEI_ARCHIVE/"
    
    if isinstance(variable_list,list) == True:
        if len(variable_list) == 1:
            variable_str = str(variable_list[0])
        else:
            variable_str = '+'.join(str(x) for x in variable_list)
    else:
        variable_str = str(variable_list)
        
    if isinstance(country_list,list) == True:
        N = len(country_list)
        if len(country_list) == 1:
            country_str = country_list[0]
        else:
            country_str = '+'.join(str(x) for x in country_list)
    else:
        N = 1;
        country_str     = country_list
    
    startTime = "startTime=" + startDate
    endTime = "endTime=" + endDate

    if startEDI == [] and endEDI == []:
        if float(startDate.replace('-','')) >= 199902:
            edition_dates = pd.date_range(datetime.strptime(startDate, '%Y-%m'),datetime.now(),freq='m').strftime('%Y%m')
        else:
            edition_dates = pd.date_range(datetime.strptime('1999-02', '%Y-%m'),datetime.now(),freq='m').strftime('%Y%m')

    elif endEDI == []:
        edition_dates = pd.date_range(datetime.strptime(startEDI, '%Y-%m'),datetime.now(),freq='m').strftime('%Y%m')
    elif startEDI == []:
        edition_dates = pd.date_range(datetime.strptime(startDate, '%Y-%m'),datetime.strptime(endEDI, '%Y-%m'),freq='m').strftime('%Y%m')
    else:
        edition_dates = pd.date_range(datetime.strptime(startEDI, '%Y-%m'),datetime.strptime(endEDI, '%Y-%m'),freq='m').strftime('%Y%m')

    edition_str = '+'.join(str(x) for x in edition_dates)
    
    url = url_base + country_str + "." + variable_str + "." + edition_str + "." + frequency + "/all?" + startTime + "&" + endTime 
  
    # ============= Download Data
    response = rq.get(url = url, params = {})

    if (response.status_code == 200):
        
        responseJson = response.json()
        # Get list of observations. This includes all revision to variables, not just real time vintages
        series = responseJson.get('dataSets')[0].get('series')
        filterKeys = lambda k: {x: series[x] for x in k}

        if (len(series) > 0):
            
            # Countries in dataset
            temp = responseJson.get('structure').get('dimensions').get('series')[0].get('values') 
            countries = [temp[i].get('id') for i in range(len(temp))]
            
            # Variables in dataset
            temp = responseJson.get('structure').get('dimensions').get('series')[1].get('values') 
            variables = [temp[i].get('id') for i in range(len(temp))]
            
            # All available time periods. Does NOT necessarily equal all time periods per country
            temp = responseJson.get('structure').get('dimensions').get('observation')[0].get('values')
            dates = [temp[i].get('id') for i in range(len(temp))]
            # Remove wrong frequency (sometimes in there by accident)
            if  frequency == 'M':
                dates = [item for item in dates if "Q" not in item]
            elif frequency == 'Q':
                dates = [item for item in dates if "M" not in item]
            # All editions in dataset (not necessarily in chronological order!!)
            temp = responseJson.get('structure').get('dimensions').get('series')[2].get('values')
            editions = [temp[i].get('id') for i in range(len(temp))]
            editions_sort = [int(item) for item in editions]
            editions_sort.sort()
            # Units of variables
            units = responseJson.get('structure').get('attributes').get('series')[1].get('values')            
            # Get all keys 
            key = list(series.keys())       
            key_id = [key[i].split(':')[2] for i in range(len(key))]
            # Create empty dict with all editions as index
            df_all = dict.fromkeys(editions_sort)
            for j in range(len(editions)):
                # Get editions per country
                splitKeys = [key[i] for i in range(len(key)) if key_id[i]==str(j)]
                tempseries = filterKeys(splitKeys)
                    
                if len(tempseries) == 0:
                        
                    print('Error: No results for requested variable no.' + variable_str + 'for country' + countries[j])
                    
                else: 
                    # Vintage for each country
                    tempVintage = [tempseries[item].get('observations') for item in splitKeys] 
                    # Keys to identify time periods
                    tempVintageKeys = [list(tempVintage[i].keys()) for i in range(len(tempVintage))]
                    # Keys to identify countries
                    tempCountryKeys = [item.split(':')[0] for item in splitKeys] 
                        
                    # Get all vintages
                    for i in range(len(tempVintage)):
                        if i == 0: 
                            tempDates = [dates[int(k)] for k in tempVintageKeys[i]]
                            df = pd.DataFrame(tempDates)
                            df.set_index(0, inplace=True)
                            tempname = countries[int(tempCountryKeys[i])] + '_' + variables[0]
                            df[tempname] = [tempVintage[i][k][0] for k in tempVintageKeys[i]]
                        else:
                            tempDates = [dates[int(k)] for k in tempVintageKeys[i]]
                            df_temp = pd.DataFrame(tempDates)
                            df_temp.set_index(0, inplace=True)
                            tempname = countries[int(tempCountryKeys[i])] + '_' + variables[0]
                            df_temp[tempname] = [tempVintage[i][k][0] for k in tempVintageKeys[i]]
                            if len(df_temp) > len(df):
                                df = df.join(df_temp,how='right')
                            else:
                                df = df.join(df_temp)
                    df_all[int(editions[j])] = df    
                    
            return df_all

        else:
            
            print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)
            
    elif (response.status_code == 404):
        
          print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)

    else:

        print('Error: %s' % response.status_code)
        print('Error: Check URL. Made request from: /r/n' + url)

def get_var_codes_MEI_BTS_COS():
    url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/MEI_BTS_COS"
    resp = rq.get(url)
    doc = etree.fromstring(resp.content)
    
    root = "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/*[@id='CL_MEI_BTS_COS_SUBJECT']/"
    var_list = doc.findall(root)
    var_name = [var_list[i].get('value') for i in range(2,len(var_list))]
    var_description = [var_list[i][0].text for i in range(2,len(var_list))]
    return var_name, var_description

def get_country_codes_MEI_BTS_COS():
    url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/MEI_BTS_COS"
    resp = rq.get(url)
    doc = etree.fromstring(resp.content)
    
    root ="{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/*[@id='CL_MEI_BTS_COS_LOCATION']/"
    country_list = doc.findall(root)
    country_code = [country_list[i].get('value') for i in range(2,len(country_list))]

    return country_code

def get_series_MEI_BTS_COS(country_list, variable_list, frequency,  startDate, endDate):     
    # Request data from OECD API and return pandas DataFrame
    
    # =============== INPUT 
    # country_list: list of countries
    # variable_list: list of variabes
    # frequency: 'M' for monthly and 'Q' for quarterly time series
    # startDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # endDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
   
    # =============== RAW DATA STRUCTURE
    # The dataset has a total of M series which are identified through four keys in the following format: 0:0:0:0
    # Position 1: Variable
    # Position 2: Country
    # Position 3: Measure
    # Position 4: Frequency
    # Each series contains n observations for each time period, identified through a number t
    # For example, for country "GBR", variable "201" with frequency M, between 1999-01 to 1999-12 the series 
    # contains 12 observations.
    # It is possible that t is not be a consecutive series of values in which case observations are missing. 
    # Code accounts for differences in length of time series.
    
    # ============= Create URL
    url_base = "https://stats.oecd.org/sdmx-json/data/MEI_BTS_COS/"
    
    if isinstance(variable_list,list) == True:
        if len(variable_list) == 1:
            variable_str = str(variable_list[0])
        else:
            variable_str = '+'.join(str(x) for x in variable_list)
    else:
        variable_str = str(variable_list)
        
    if isinstance(country_list,list) == True:
        N = len(country_list)
        if len(country_list) == 1:
            country_str = country_list[0]
        else:
            country_str = '+'.join(str(x) for x in country_list)
    else:
        N = 1;
        country_str = country_list
    
    startTime = "startTime=" + startDate
    endTime = "endTime=" + endDate
    measure = "BLSA"
    
    url = url_base + variable_str + "." + country_str + "." + measure + "." + frequency + "/all?" + startTime + "&" + endTime 
  
    # ============= Download Data
    response = rq.get(url = url, params = {})

    if (response.status_code == 200):
        
        responseJson = response.json()
        # Get list of observations. This includes all revision to variables, not just real time vintages
        series = responseJson.get('dataSets')[0].get('series')
        filterKeys = lambda k: {x: series[x] for x in k}

        if (len(series) > 0):
            
            # All variables in dataset
            temp = responseJson.get('structure').get('dimensions').get('series')[0].get('values') 
            variables = [temp[i].get('id') for i in range(len(temp))]
            
            # All available time periods. Does NOT necessarily equal all time periods per country/variable
            temp = responseJson.get('structure').get('dimensions').get('observation')[0].get('values')
            dates = [temp[i].get('id') for i in range(len(temp))]
            dates = [item for item in dates if "Q" not in item]
            
            # All measure (all the same)
            temp = responseJson.get('structure').get('dimensions').get('series')[2].get('values')
            measure = [temp[i].get('id') for i in range(len(temp))]
            
            # Countries 
            temp = responseJson.get('structure').get('dimensions').get('series')[1].get('values') 
            countries = [temp[i].get('id') for i in range(len(temp))]

            if N == 1:
                temp = list(series.values())      
                tempObs = [temp[i].get('observations') for i in range(len(temp))]  
                tempKeys = [list(tempObs[i].keys()) for i in range(len(tempObs))]
                
                realObs = []
                for i in range(len(tempObs)):
                    if i == 0:
                        realObs.extend([tempObs[0][j][0] for j in set(tempKeys[0])])
                    else:
                        newKey = set(tempKeys[i]) - set().union(*tempKeys[0:i]);
                        if len(newKey) > 0:
                            realObs.extend([tempObs[i][j][0] for j in newKey])
                    
                df = pd.DataFrame(dates)
                df[countries[0]] = realObs
                return df

            elif len(variables) > 1:
            
                # Get all keys
                key = list(series.keys())       
                var_key_id = [key[i].split(':')[0] for i in range(len(key))]
                # Create empty dataframe with all dates as index
                df = pd.DataFrame(dates)
                df.set_index(0, inplace=True)
                
                # Combine Data per variable for each country
                df_all = dict.fromkeys(variables)
                for j in range(len(variables)):
                    
                    df = pd.DataFrame(dates)
                    df.set_index(0, inplace=True)
                
                    # Get series per country
                    splitKeys = [key[i] for i in range(len(key)) if var_key_id[i]==str(j)]
                    tempseries = filterKeys(splitKeys)
                    temp = list(tempseries.values())   
                    
                    if len(temp) == 0:
                        
                        print('Error: No results for requested variable' + variable_list[j])
                    
                    else: 
                         # Get country keys
                        subkey = list(tempseries.keys())       
                        country_key_id = [subkey[i].split(':')[1] for i in range(len(subkey))]
                        # All observations for each country
                        tempObs = [temp[i].get('observations') for i in range(len(temp))]  
                        # Keys (t) to identify time periods
                        tempKeys = [list(tempObs[i].keys()) for i in range(len(tempObs))]
                        for i in range(len(tempObs)):
                            # Get Observations
                            Obs = [item[0] for item in list(tempObs[i].values())]
                            # Get dates for observations
                            tempDates = [dates[int(x)] for x in tempKeys[i]]
                            df_temp = pd.DataFrame(tempDates)
                            df_temp.set_index(0, inplace=True)
                            # Get country name
                            tempname = countries[int(country_key_id[i])] + '_' + variables[j]
                            df_temp[tempname] = Obs
                            # Combine with countries from previous iteration, fill missing dates with nan
                            df = df.join(df_temp)
                        df_all[variables[j]] = df
                return df_all

        else:
            
            print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)
            
    elif (response.status_code == 404):
        
          print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)

    else:

        print('Error: %s' % response.status_code)
        print('Error: Check URL. Made request from: /r/n' + url)
        
        

def get_var_codes_MEI_FIN():
    url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/MEI_FIN"
    resp = rq.get(url)
    doc = etree.fromstring(resp.content)
    
    root = "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/*[@id='CL_MEI_FIN_SUBJECT']/"
    var_list = doc.findall(root)
    var_name = [var_list[i].get('value') for i in range(2,len(var_list))]
    var_description = [var_list[i][0].text for i in range(2,len(var_list))]
    return var_name, var_description

def get_country_codes_MEI_FIN():
    url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/MEI_FIN"
    resp = rq.get(url)
    doc = etree.fromstring(resp.content)
    
    root ="{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/*[@id='CL_MEI_FIN_LOCATION']/"
    country_list = doc.findall(root)
    country_code = [country_list[i].get('value') for i in range(2,len(country_list))]

    return country_code

def get_series_MEI_FIN(country_list, variable_list, frequency,  startDate, endDate):     
    # Request data from OECD API and return pandas DataFrame
    
    # =============== INPUT 
    # country_list: list of countries
    # variable_list: list of variabes
    # frequency: 'M' for monthly and 'Q' for quarterly time series
    # startDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
    # endDate: date in YYYY-MM (2000-01) or YYYY-QQ (2000-Q1) format, None for all observations
   
    # =============== RAW DATA STRUCTURE
    # The dataset has a total of M series which are identified through four keys in the following format: 0:0:0:0
    # Position 1: Variable
    # Position 2: Country
    # Position 3: Measure
    # Position 4: Frequency
    # Each series contains n observations for each time period, identified through a number t
    # For example, for country "GBR", variable "201" with frequency M, between 1999-01 to 1999-12 the series 
    # contains 12 observations.
    # It is possible that t is not be a consecutive series of values in which case observations are missing. 
    # Code accounts for differences in length of time series.
    
    # ============= Create URL
    url_base = "https://stats.oecd.org/sdmx-json/data/MEI_FIN/"
    
    if isinstance(variable_list,list) == True:
        if len(variable_list) == 1:
            variable_str = str(variable_list[0])
        else:
            variable_str = '+'.join(str(x) for x in variable_list)
    else:
        variable_str = str(variable_list)
        
    if isinstance(country_list,list) == True:
        N = len(country_list)
        if len(country_list) == 1:
            country_str = country_list[0]
        else:
            country_str = '+'.join(str(x) for x in country_list)
    else:
        N = 1;
        country_str = country_list
    
    startTime = "startTime=" + startDate
    endTime = "endTime=" + endDate
    
    url = url_base + variable_str + "." + country_str + "." + frequency + "/all?" + startTime + "&" + endTime 
  
    # ============= Download Data
    response = rq.get(url = url, params = {})

    if (response.status_code == 200):
        
        responseJson = response.json()
        # Get list of observations. This includes all revision to variables, not just real time vintages
        series = responseJson.get('dataSets')[0].get('series')
        filterKeys = lambda k: {x: series[x] for x in k}

        if (len(series) > 0):
            
            # All variables in dataset
            temp = responseJson.get('structure').get('dimensions').get('series')[0].get('values') 
            variables = [temp[i].get('id') for i in range(len(temp))]
            
            # All available time periods. Does NOT necessarily equal all time periods per country/variable
            temp = responseJson.get('structure').get('dimensions').get('observation')[0].get('values')
            dates = [temp[i].get('id') for i in range(len(temp))]
            dates = [item for item in dates if "Q" not in item]
            
            # All measure (all the same)
            temp = responseJson.get('structure').get('dimensions').get('series')[2].get('values')
            measure = [temp[i].get('id') for i in range(len(temp))]
            
            # Countries 
            temp = responseJson.get('structure').get('dimensions').get('series')[1].get('values') 
            countries = [temp[i].get('id') for i in range(len(temp))]
            
            # Get all keys
            key = list(series.keys())       
            var_key_id = [key[i].split(':')[0] for i in range(len(key))]
            # Create empty dataframe with all dates as index
            df = pd.DataFrame(dates)
            df.set_index(0, inplace=True)
                
            # Combine Data per variable for each country
            df_all = dict.fromkeys(variables)
            for j in range(len(variables)):
                    
                df = pd.DataFrame(dates)
                df.set_index(0, inplace=True)
                
                # Get series per country
                splitKeys = [key[i] for i in range(len(key)) if var_key_id[i]==str(j)]
                tempseries = filterKeys(splitKeys)
                temp = list(tempseries.values())   
                    
                if len(temp) == 0:
                        
                    print('Error: No results for requested variable' + variable_list[j])
                    
                else: 
                    # Get country keys
                    subkey = list(tempseries.keys())       
                    country_key_id = [subkey[i].split(':')[1] for i in range(len(subkey))]
                    # All observations for each country
                    tempObs = [temp[i].get('observations') for i in range(len(temp))]  
                    # Keys (t) to identify time periods
                    tempKeys = [list(tempObs[i].keys()) for i in range(len(tempObs))]
                    for i in range(len(tempObs)):
                        # Get Observations
                        Obs = [item[0] for item in list(tempObs[i].values())]
                        # Get dates for observations
                        tempDates = [dates[int(x)] for x in tempKeys[i]]
                        df_temp = pd.DataFrame(tempDates)
                        df_temp.set_index(0, inplace=True)
                        # Get country name
                        tempname = countries[int(country_key_id[i])] + '_' + variables[j]
                        df_temp[tempname] = Obs
                        # Combine with countries from previous iteration, fill missing dates with nan
                        df = df.join(df_temp)
                    df_all[variables[j]] = df
            return df_all

        else:
            
            print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)
            
    elif (response.status_code == 404):
        
          print('Error: No results for requested variable no. ' + variable_str + ' for country ' + country_str)

    else:

        print('Error: %s' % response.status_code)
        print('Error: Check URL. Made request from: /r/n' + url)
        
def merge_MEI_Vintage(MEI_ALL,transform):
    
    if transform == []:
        
        nVar = len(MEI_ALL)
        allKeys = [list(item.keys()) for item in MEI_ALL]
        temp = list(set().union(*allKeys))
        allEditions = [int(item) for item in temp]
        allEditions.sort()
        allVintages = dict.fromkeys(allEditions)
        for j in allEditions:
            tempEd = []
            for x in MEI_ALL:
                try:
                    tempEd.append(x[j])
                except:
                    pass
                for i in range(len(tempEd)):
                    if i == 0:
                        df = tempEd[i]
                    else:
                        if len(tempEd[i]) > len(df):
                            df = df.join(tempEd[i],how='right')
                        else:
                            df = df.join(tempEd[i])
            allVintages[j] = df        
        return allVintages
    
    else:
        
        nVar = len(MEI_ALL)
        allKeys = [list(item.keys()) for item in MEI_ALL]
        temp = list(set().union(*allKeys))
        allEditions = [int(item) for item in temp]
        allEditions.sort()
        allVintages = dict.fromkeys(allEditions)
        for j in allEditions:
            tempEd = []
            for x in MEI_ALL:
                try:
                    tempEd.append(x[j])
                except:
                    pass
                for i in range(len(tempEd)):
                    if i == 0:
                        df = tempEd[i]
                    else:
                        if len(tempEd[i]) > len(df):
                            df = df.join(tempEd[i],how='right')
                        else:
                            df = df.join(tempEd[i])
            trans = [transform] * len(df.columns)
            trans = pd.DataFrame([trans],columns=df.columns)
            trans.index = ['Transform']
            df = pd.concat([trans,df])
            allVintages[j] = df           
        return allVintages

def merge(data):
    tempKeys = list(data.keys())
    for i in range(len(tempKeys)):
        if i == 0:
            data_new = data[tempKeys[i]]
        else:
            if len(data[tempKeys[i]]) > len(data_new):
                data_new = data_new.join(data[tempKeys[i]],how='right')
            else:
                data_new = data_new.join(data[tempKeys[i]])
    return data_new