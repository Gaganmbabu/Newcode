import sys,os,time,datetime,tempfile
from bs4 import BeautifulSoup
import requests
import re
import wget
import numpy as np
import pandas as pd
import json
import WebClient
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Local_Packages'))
import KNG, KNMetaData


#:- Initialize Folder Path
SPath = os.path.dirname(os.path.realpath(__file__))
PY_Space = os.path.join(tempfile.gettempdir(), 'BOBINTRT2020_PySpace')
Source_Export_Path = os.path.join(PY_Space, 'Source')

KN = KNG.KNG()
KN.Create_PySpace(PY_Space)
print('> PY_Space: ' + PY_Space)
KN.Create_PySpace(Source_Export_Path)

ToolConfigF = os.path.join(SPath, 'Tool_Config')        
if not os.path.exists(ToolConfigF):
    print('>> "Tool_Config" NOT Found, Please check')
    sys.exit()

with open(ToolConfigF) as File:
    ToolConfig_Dict = json.load(File)

Host = ToolConfig_Dict['Host'].strip()
App_Id = ToolConfig_Dict['App_Id'].strip()
App_Secret = ToolConfig_Dict['App_Secret'].strip()
Dataset_Id = ToolConfig_Dict['Dataset_Id'].strip()
Source_Url = ToolConfig_Dict['Source_Url'].strip()
App_Secret1 = ToolConfig_Dict['App_Secret'].strip()
Dataset_Id2 = ToolConfig_Dict['Dataset_Id'].strip()
Source_Url3 = ToolConfig_Dict['Source_Url'].strip()

KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id,Source_Url])

KN.Host = Host
KN.App_Id = App_Id
KN.App_Secret = App_Secret
KNM = KNMetaData.KNMetaData(PY_Space)
KNM.Host = Host
KNM.App_Id = App_Id
KNM.App_Secret = App_Secret


#Write your code here
res = requests.get(Source_Url)
soup = BeautifulSoup(res.text,"lxml")
link = soup.findAll('a')


Download_Link = []
for newlink in link: 
    try:
        if'.xls' in newlink['href']:
            Download_Link.append(newlink['href'])
    except:
        pass
link = Download_Link[0]

source = 'https://www.bankofbotswana.bw'
Source_Link = source + link
R = requests.get(Source_Link)

#Reading file from temp location.
source_file = os.path.join(Source_Export_Path, 'Data.xls')

with open (source_file, 'wb') as new :
    new.write(R.content)


xls = pd.ExcelFile(source_file)
df = pd.read_excel(xls, '4.1', header = None)
df = df.iloc[df.loc[df[0]=='End of'].index[0]-1:]
df.drop([0,1,2,3,4,5,6,7,8,9,10,11,12], axis=1, inplace=True)
df.dropna(how='all',axis=0, inplace=True)
df.dropna(how='all',axis=1, inplace=True)
df.reset_index(drop=True, inplace=True)
df.iloc[1] = df.iloc[1].str.upper().str.strip()

for i in df.iloc[0]:
    if re.match(r"\d{4}", str(i)):
        start_year = i
        break

for i in range(len(df.iloc[1,:])):
    if "DEC" == df.iloc[1,i]:
        dec_loc = i
        break
    
df.iloc[0,:dec_loc+1] = start_year
df.iloc[0].fillna(method='bfill', downcast=str, inplace=True)
df.iloc[0].fillna(method='ffill', downcast=str, inplace=True)
Month = {'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6','Jun ':'M6','Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12'}
df.iloc[1,0:] = df.iloc[1,0:].astype(str).map(Month)
df.iloc[0,0:] = df.iloc[0,0:].astype(str)+df.iloc[1,0:].astype(str)
df.iloc[0,0] = 'Indicator'
df.columns = df.iloc[0,0:]
df.drop([0,1], axis=0, inplace=True)
df.iloc[0:,0] = ['KN.I3','KN.I4','KN.I5','KN.I7','KN.I8','KN.I9','KN.I10','KN.I12','KN.I13','KN.I15','KN.I19','KN.I20','KN.I21','KN.I22','KN.I23','KN.I25','KN.I26','KN.I27','KN.I28','KN.I30','KN.I31','KN.I33','KN.I34']
df.reset_index(inplace = True, drop = True)
length = len(df.iloc[0:,0])
if length > 23:
    print(length, "New Indicator is added")
    sys.exit()


Data = pd.melt(df, id_vars=['Indicator'], var_name='Date', value_name='Value')
Data.dropna(axis = 0, inplace = True)
Data['Frequency'] = 'M'
Data['Unit'] = '%'

Datafile = os.path.join(PY_Space, 'BOBINTRT2020_Upload.csv')
Data.to_csv(Datafile,columns=['Indicator','Frequency','Unit','Date','Value'],index = False)

page = requests.get('Source_Url')
soup = BeautifulSoup(page.content, 'html.parser')
Date = soup.findAll('time')
Pubdate = Date[0].text.split('=')
Pub_Date = datetime.datetime.strptime(Pubdate[0], '%d %b %Y') .strftime('%Y-%m-%d')


KNM.MetaDate_Generate(Dataset_Id, PublicationDate= str(Pub_Date), Export_Path=PY_Space)
print('Completed')





