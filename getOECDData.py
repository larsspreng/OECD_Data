# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 17:09:55 2022

File to download (a) real time data from OECD MEI Archive and (b) Survey Data from the OECD Business & Consumer Survey.
This file can also be used to download the exact data vintages used in Hillebrand, Mikkelsen, Spreng, and Urga (2023) 
Exchange Rates and Macroeconomic Fundamentals: Evidence of Instabilities from Time-Varying Factor Loadings

@author: Lars E. Spreng
"""

import pandas as pd 
import numpy as np
from datetime import datetime
import OECDData as OECD
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

""" ====================== Monthly Economic Indicators ====================== """
# ========= Settings
# Path to save dataset
path = r'C:\Users\aczz248\OneDrive - City, University of London\Code\OECD Data\Data'
# Get all available variables in MEI Archive
variable_list,variable_names = OECD.get_var_codes_MEIArchive();
# Get all available countries in MEI Archive 
country_list = OECD.get_country_codes_MEIArchive();
# Remove GDP
variable_list = variable_list[8:22]
variable_names = variable_names[8:22]
# Remove Composite Lending Indicator (CLI)
remove = [2,3,4,8]
variable_list = [v for i, v in enumerate(variable_list) if i not in remove]
variable_names = [v for i, v in enumerate(variable_names) if i not in remove]
# List of countries (commetn out to use list from above if you want all countries)
country_list = ["AUS", "CAN", "DNK", "JPN", "MEX", "NZL", "NOR", "SWE", "CHE", "BRA", "IND", "ZAF", "GBR",
                  "FRA", "DEU", "ITA"]
# Frequency (M for monthly, Q for Quarterly)
frequency = "M"
# Set the start date and end date of the first and last vintage
startDate = '1990-01'
endDate = '2023-01'
# Get the current date
currentDate = datetime.today().strftime('%Y-%m-%d')
# Edition of the dataset, i.e. publishing date (leave empty for real time data)
startEDI = []
endEDI = []
# Categories
category_list = [1,1,1,4,7,2,2,2,5,1];
category_match = pd.DataFrame(variable_list)
category_match.set_index(0, inplace=True)
category_match['Category'] = category_list

# ========== Download First-Release Data from Monthly Economic Indicator Archive 
MEI_RT = []
units_list = []
for j in range(8,len(variable_list)):
    temp,units = OECD.get_series_first_release_MEIArchive(country_list, variable_list[j], frequency,  startDate, endDate, startEDI, endEDI);    
    if temp is not None:
        MEI_RT.append(temp)
        units_list.append(units)

# ========== Download All Vintages from Monthly Economic Indicator Archive 
MEI_ALL = []
for j in range(4,len(variable_list)):
    temp = OECD.get_series_all_releases_MEIArchive(country_list, variable_list[j], frequency,  startDate, endDate, startEDI, endEDI);
    if temp is not None:
        MEI_ALL.append(temp)
        
# ========== Merge Data Together
MEI_new = OECD.merge_MEI_Vintage(MEI_ALL,5)

# ========== Documentation Table for all vintages
latex_table = ' \documentclass{article} '
latex_table = latex_table + ' \\usepackage[utf8]{inputenc}' + ' \\title{OECD MEI Documentation}' + " \\author{Lars Spreng} " +  " date{" + currentDate + "} "
latex_table = latex_table + ' \\begin{document} ' +  '\maketitle ' + ' \\section{MEI Data}' + ' \\begin{landscape}'

editions_list = list(MEI_ALL[1].keys())
for i in editions_list:
    documentation_table = pd.DataFrame(variable_names)
    documentation_table.set_index(0, inplace=True)
    documentation_table['Cat.'] = category_list
    for j in range(len(MEI_ALL)):
        temp_country = [x[0:3] for x in MEI_ALL[j][i].columns]
        #temp_var = variable_names[variable_list.index(int(MEI_ALL[j][i].columns[1][4:]))]
        temp_var = variable_list.index(int(MEI_ALL[j][i].columns[1][4:]))
        first_date = MEI_ALL[j][i].apply(pd.Series.first_valid_index)
        last_date = MEI_ALL[j][i].apply(pd.Series.last_valid_index)
        temp_df = pd.DataFrame(temp_country)
        temp_df.set_index(0, inplace=True)
        temp_df[temp_var] = 'to ' + last_date.values
        if j == 0:
            country_df = temp_df;
        elif j > 0:   
            if len(country_df) > len(temp_df):
                country_df = country_df.join(temp_df,how='left')
            elif len(country_df) <= len(temp_df):
                country_df = country_df.join(temp_df,how='right')
                country_df.fillna('N/A', inplace=True)  
    documentation_table = documentation_table.join(country_df.T) 
    documentation_table = documentation_table.dropna()
    documentation_table = documentation_table.reset_index(level=0) 
    documentation_table = documentation_table.rename(columns={0:'Var.'})     
    columns = ['@{}','l','l']              
    columns = columns + (['Y'] * len(temp_country)) + ['@{}']
    columns = ''.join(str(e) for e in columns)
    latex_table = latex_table + ' \clearpage ' + ' ' + documentation_table.to_latex(index=False,caption="Edition: " + str(i), column_format =columns )

latex_table = latex_table + '\\end{landscape} ' + ' \\end{document}'
text_file = open("OECD_Doc/MEI_Documentation_" + currentDate + ".tex", "w")
n = text_file.write(latex_table)
text_file.close()
        
# ========== Documentation Table for 2022-02 Vintage used for In-Sample Estimation in Hillebrand et al. (2022)
documentation_table = pd.DataFrame(variable_names)
documentation_table.set_index(0, inplace=True)
documentation_table['Cat.'] = category_list
for j in range(len(MEI_ALL)):
    temp_country = [x[0:3] for x in MEI_ALL[j][202202].columns]
    temp_var = variable_names[variable_list.index(int(MEI_ALL[j][202202].columns[1][4:]))]
    first_date = MEI_ALL[j][202202].apply(pd.Series.first_valid_index)
    last_date  = MEI_ALL[j][202202].apply(pd.Series.last_valid_index)
    temp_df = pd.DataFrame(temp_country)
    temp_df.set_index(0, inplace=True)
    temp_df[temp_var] = first_date.values + ' to ' + last_date.values
    if j == 0:
        country_df = temp_df;
    elif j > 0:   
        if len(country_df) > len(temp_df):
            country_df = country_df.join(temp_df,how='left')
        elif len(country_df) <= len(temp_df):
            country_df = country_df.join(temp_df,how='right')
    country_df.fillna('N/A', inplace=True)  
documentation_table = documentation_table.join(country_df.T) 
documentation_table = documentation_table.dropna()
documentation_table = documentation_table.reset_index(level=0) 
documentation_table = documentation_table.rename(columns={0:'Series'})     
columns = ['@{}','l','l']              
columns = columns + (['Y'] * len(temp_country)) + ['@{}']
columns = ''.join(str(e) for e in columns)
latex_table = documentation_table.to_latex(index=False,caption="Main Economic Indicatiors", column_format =columns )
text_file = open("OECD_Doc/MEI_Documentation_202202.tex", "w")
n = text_file.write(latex_table)
text_file.close()
        
        
""" ====================== Survey Indicators ====================== """
# =========== Settings
# Get all available variable names
variable_list,variable_names = OECD.get_var_codes_MEI_BTS_COS()
# Get all available countries
country_list = OECD.get_country_codes_MEI_BTS_COS()
# List of countries (use list from above if you want all countries)
country_list = ["AUS", "CAN", "DNK", "JPN", "MEX", "NZL", "NOR", "SWE", "CHE", "BRA", "IND", "ZAF", "GBR",
                   "FRA", "DEU", "ITA"]

# =========== Download Survey Data from Monthly Economic Indicator Archive        
BTS_COS = OECD.get_series_MEI_BTS_COS(country_list, variable_list, frequency, startDate, endDate)

# =========== Full variable names
idx = [variable_list.index(item) for item in list(BTS_COS.keys())]
sublevel = [variable_list[item] for item in idx]
sublevel_ex = [item for item in sublevel if len(item) > 4]
categories = [item for item in variable_list if len(item) == 2]
full_variable_names = [];

for i in categories:
    temp_list = [x for x in variable_list if i in x[0:2]]
    if len(temp_list[1:]) > 0:
        is_in = [x not in sublevel_ex for x in temp_list]
        temp_idx = variable_list.index(i)
        for j in range(len(temp_list[1:])): 
            if is_in[j+1]:      
               subidx  = variable_list.index(temp_list[j+1])
               sublist = [x for x in sublevel if temp_list[j+1] in x]
               subsubidx = [variable_list.index(item) for item in sublist]
               full_variable_names.extend(
                    [variable_names[temp_idx] + ' ' + variable_names[subidx] + ' ' + variable_names[item] for item in subsubidx])

BTS_COS_new = OECD.merge(BTS_COS)

# Categories
category_list = [1,1,1,4,4,4,7,2,1,1,1,1,1,1,4,2,7,1,1,1,4,2,4,1,1,1,1,2,2,1,1,7];
category_match_BTS_COS = pd.DataFrame(sublevel)
category_match_BTS_COS.set_index(0, inplace=True)
category_match_BTS_COS['Category'] = category_list

# Add transformation
transform = [5] * len(BTS_COS_new.columns)
transform = pd.DataFrame([transform],columns=BTS_COS_new.columns)
transform.index = ['Transform']
BTS_COS_new = pd.concat([transform,BTS_COS_new])

# ========== Documentation Table 
documentation_table = pd.DataFrame(full_variable_names)
documentation_table.set_index(0, inplace=True)
documentation_table['Cat.'] = category_list
BTS_COS_keys = list(BTS_COS.keys())
for j in range(len(BTS_COS)):
    temp_country = [x[0:3] for x in BTS_COS[BTS_COS_keys[j]].columns]
    temp_var = full_variable_names[j]
    first_date = BTS_COS[BTS_COS_keys[j]].apply(pd.Series.first_valid_index)
    last_date = BTS_COS[BTS_COS_keys[j]].apply(pd.Series.last_valid_index)
    temp_df = pd.DataFrame(temp_country)
    temp_df.set_index(0, inplace=True)
    temp_df[temp_var] = first_date.values + ' to ' + last_date.values
    if j == 0:
        country_df = temp_df;
    elif j > 0:   
        if len(country_df) > len(temp_df):
            country_df = country_df.join(temp_df,how='left')
        elif len(country_df) <= len(temp_df):
            country_df = country_df.join(temp_df,how='right')
    country_df.fillna('N/A', inplace=True)  
documentation_table = documentation_table.join(country_df.T) 
documentation_table = documentation_table.dropna()
documentation_table = documentation_table.reset_index(level=0) 
documentation_table = documentation_table.rename(columns={0:'Series'})     
columns = ['@{}','l','l']              
columns = columns + (['Y'] * len(temp_country)) + ['@{}']
columns = ''.join(str(e) for e in columns)
latex_table = documentation_table.to_latex(index=False,caption="Business Tendency and Consumer Opinion Survey", column_format =columns )
text_file = open("OECD_Doc/BTS_COS_Documentation_"+currentDate+".tex", "w")
n = text_file.write(latex_table)
text_file.close()


""" ====================== Interest Rates ====================== """
# =========== Settings
# Get all available variable names
variable_list,variable_names = OECD.get_var_codes_MEI_FIN()
# Get all available countries
country_list = OECD.get_country_codes_MEI_FIN()
# List of countries (use list from above if you want all countries)
country_list = ["AUS", "CAN", "DNK", "JPN", "MEX", "NZL", "NOR", "SWE", "CHE", "BRA", "IND", "ZAF", "GBR",
                 "FRA", "DEU", "ITA"]
# Variable List (short-term & long-term Interest Rates)
variable_list = variable_list[1:3]
variable_names = variable_names[1:3]
# Categories
category_list = [6,6]
category_match_IR = pd.DataFrame(variable_list)
category_match_IR.set_index(0, inplace=True)
category_match_IR['Category'] = category_list
# ========== Download Interest Rate Data from Monthly Economic Indicator Archive        
IR = OECD.get_series_MEI_FIN(country_list, variable_list, frequency,  startDate, endDate)
IR_new = OECD.merge(IR)
# Add transformation
transform = [2] * len(IR_new.columns)
transform = pd.DataFrame([transform],columns=IR_new.columns)
transform.index = ['Transform']
IR_new = pd.concat([transform,IR_new])

""" ====================== Exchange Rates ====================== """
variable_list = ["CCUS"]
# List of countries (use list from above if you want all countries)
country_list = ["AUS", "CAN", "DNK", "JPN", "MEX", "NZL", "NOR", "SWE", "CHE", "BRA", "IND", "ZAF", "GBR",
                 "EA19"]
FX = OECD.get_series_MEI_FIN(country_list, variable_list, frequency,  startDate, endDate)
FX = OECD.merge(FX)

""" ====================== Merge Datasets ====================== """
# Merge with Interest Rates and Surveys
if len(IR_new) > len(BTS_COS_new):
    Data = IR_new.join(BTS_COS_new)
else:
    Data = BTS_COS_new.join(IR_new)
# Merge with Vintages
allData = dict.fromkeys(list(MEI_new.keys()))
for i in list(MEI_new.keys()):
    allData[i] = MEI_new[i].join(Data)

category_all = pd.concat([category_match,category_match_BTS_COS])
category_all = pd.concat([category_all,category_match_IR]) #category_match.concat(category_match_IR)

""" ====================== Save Datasets ====================== """
for i in list(MEI_new.keys()):
    allData[i].to_csv(path + "\\Historical_OECD\\" + str(i) + ".csv")

FX.to_csv(path + "\\OECD_FX.csv")

category_all.to_csv(path + "\\OECD_categories.csv")



