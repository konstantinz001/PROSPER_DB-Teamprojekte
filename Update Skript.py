## Bibliotheken:
import re
import pyodbc as odbc 
from typing import TypeVar, Generic

from datetime import datetime
import locale
from sqlite3 import Cursor

import requests
from bs4 import BeautifulSoup
from lxml import etree
import pandas
import unicodedata
from time import sleep
import warnings
warnings.filterwarnings("ignore")
import sys

# #Parameter
# RELEASEYEAR_START = 2005
# RELEASEYEAR_END = 2022
# PUB = 'Dissertation'
# start_Position = 0
#Parameter
RELEASEYEAR_START = int(sys.argv[1])
RELEASEYEAR_END = int(sys.argv[2])
PUB = str(sys.argv[3])
start_Position = int(sys.argv[4])


DB_NAME_PREFIX = '****'

connection_string = "******"

conn = odbc.connect(connection_string)

#Execute
def executeSQL1(conn, sqlQuery):
    cursor = conn.cursor()
    cursor.execute(sqlQuery)
    conn.commit()

def executeSQL(conn, sqlQuery, parameter: TypeVar):
    cursor = conn.cursor()
    cursor.execute(sqlQuery,parameter)
    conn.commit()

def sqlToDataframe(conn, sqlQuery):
    cursor = conn.cursor()
    #"Select DNB_IDN, CREATOR, TITLE, ISBN, ERSCHEINUNGSJAHR, PUBLICATIONSART, SACHGRUPPE FROM DNB_DataSets"
    resultSQL = cursor.execute(sqlQuery)
    return pandas.DataFrame(resultSQL.fetchall())

def metaDict_To_DataFrame():    
    result = [get_meta_dict(item) for item in records_marc]
    df = pandas.DataFrame(result).drop_duplicates(subset=['CREATOR', 'TITLE'])              
    return df


#FORMAT
def get_meta_dict(item):
    ns = {"marc":"http://www.loc.gov/MARC21/slim"}
    xml = etree.fromstring(unicodedata.normalize("NFC", str(item)))
    
    #idn
    idn = xml.findall("marc:controlfield[@tag = '001']", namespaces=ns)
    try:
        idn = idn[0].text
    except:
        try:
            idn = idn.text
        except:
            idn = ''

    #sgt
    sgt = ''
    sgt1 = xml.findall("marc:datafield[@tag = '082']/marc:subfield[@code = 'a']", namespaces=ns)
    try:
        sgt1= str(sgt1[0].text)
    except:
        try:
            sgt1 = str(sgt1.text)
        except:
            sgt1 = ''

    sgt3 = xml.findall("marc:datafield[@tag = '084']/marc:subfield[@code = 'a']", namespaces=ns)
    try:
        sgt3 = str(sgt3[0].text)
    except:
        try:
            sgt3 = str(sgt3.text)
        except:
            sgt3 = ''


    sgt2 = xml.findall("marc:datafield[@tag = '083']/marc:subfield[@code = 'a']", namespaces=ns)
    try:
        sgt2 = str(sgt2[0].text)
    except:
        try:
            sgt2 = str(sgt2.text)
        except:
            sgt2 = ''


    sgt = set([sgt1, sgt2, sgt3])
    sgt = '|'.join(sgt)
    if(sgt.startswith('|')):
        sgt=sgt[1:]
    if(sgt.endswith('|')):
        sgt = sgt[:-1]
       
    #creator
    creator1 = xml.findall("marc:datafield[@tag = '100']/marc:subfield[@code = 'a']", namespaces=ns)
    creator2 = xml.findall("marc:datafield[@tag = '110']/marc:subfield[@code = 'a']", namespaces=ns)
    creator3 = xml.findall("marc:datafield[@tag = '700']/marc:subfield[@code = 'a']", namespaces=ns)
    
    if creator1:
        creator = creator1[0].text
    elif creator2:
        creator = creator2[0].text
    elif creator3:
        creator = creator3[0].text
    else:
        creator = ''
    
    #Titel $a
    title = xml.findall("marc:datafield[@tag = '245']/marc:subfield[@code = 'a']", namespaces=ns)
    title2 = xml.findall("marc:datafield[@tag = '245']/marc:subfield[@code = 'b']", namespaces=ns)
    
    if title and not title2:
        titletext = title[0].text
    elif title and title2:     
        titletext = title[0].text + ": " + title2[0].text
    else:
        titletext = ""
    
    #date
    date1TMP = ''
    date2TMP = ''
    date1 = xml.findall("marc:datafield[@tag = '260']/marc:subfield[@code = 'c']", namespaces=ns)
    try:
        date1TMP =date1[0].text
        date1 = re.findall(r"19[5-9][0-9]|20[0-2][0-9]", date1[0].text)
        date1 = date1[0]
    except:
      date1 = ""

    date2 = xml.findall("marc:datafield[@tag = '264']/marc:subfield[@code = 'c']", namespaces=ns)
    try:
        date2TMP =date2[0].text
        date2 = re.findall(r"19[5-9][0-9]|20[0-2][0-9]", date2[0].text)
        date2 = date2[0]
        
    except:
      date2 = ""

    if date1:
        date = date1
    elif date2:
        date = date2
    elif date1TMP:
        date = date1TMP
    elif date2TMP:
        date = date2TMP
    else:
        date = ''
      
    meta_dict = {
        "DNB_IDN":idn, 
        "CREATOR":creator, 
        "TITLE": titletext, 
        "ERSCHEINUNGSJAHR":date, 
        "PUBLICATIONSART": PUB, #CHANGE
        "SACHGRUPPE": sgt
    }
    return meta_dict


#SUCHE
def enquiryDNB(start_position=start_Position):   
    while True:  
        yearFilter = "jhr=(" + str(releasyear) + ")"
        catalogFilter = 'catalog=dnb.hss'
        if(PUB == 'Dissertation'):
            query = catalogFilter + " and " + yearFilter + " and hss=diss*" #CHANGE PUBLICATIONSART
        elif(PUB == 'Habilitation'):
            query = catalogFilter + " and " + yearFilter + " and hss=habil*"
            
        parameter = {'version' : '1.1' , 'operation' : 'searchRetrieve' , 'query' : query, 'startRecord': start_position, 'recordSchema' : 'MARC21-xml', 
                    'maximumRecords': '100'} 
        try:
            r1 = requests.get("https://services.dnb.de/sru/dnb", params = parameter)  
            return r1   
        except:
            True
            

#User input



#Ausführen
releasyear = 0
for year in range(RELEASEYEAR_START, RELEASEYEAR_END+1):

    releasyear = year
    DB_Name = DB_NAME_PREFIX + str(year)

    print("YEAR:",releasyear)
    executeSQL1(conn,
        f"""
        if not exists (select * from sysobjects where name='{DB_Name}' and xtype='U')
            CREATE TABLE {DB_Name}(
                [DNB_IDN] [varchar](50) NOT NULL,
                [CREATOR] [varchar](50) NULL,
                [TITLE] [varchar](500) NULL,
                [ERSCHEINUNGSJAHR] [varchar](25) NULL,
                [PUBLICATIONSART] [varchar](25) NULL,
                [SACHGRUPPE] [varchar](50) NULL,
                [CHECKED] [bit] NULL,
                PRIMARY KEY (DNB_IDN)
            )
        """)

    r_num = enquiryDNB()
    response = BeautifulSoup(r_num.content,features="lxml")
    numRecords = int(response.find('numberofrecords').text)  
    print('TOTAL:' + str(numRecords))   
    err = 0
    while (start_Position <= numRecords):
        
        #CHANGE PUBLICATIONSART
        try:
            req = enquiryDNB(start_Position)
            response = BeautifulSoup(req.content)
            records_marcTMP = response.find_all('record', {'type':'Bibliographic'})
            records_marc=records_marcTMP  
            df = metaDict_To_DataFrame()
            print("POSITION:"+str(start_Position))
            start_Position+= 100
        except:
            print("POSITION:"+str(start_Position))
            start_Position += 1
        for index, row in df.iterrows():
            try:
                executeSQL(conn,
                        f"INSERT INTO {DB_Name} " +
                        "(DNB_IDN, CREATOR, TITLE, ERSCHEINUNGSJAHR, PUBLICATIONSART, SACHGRUPPE, CHECKED) "+ 
                        "values(?,?,?,?,?,?, 0)", #CHANGE PUBLICATIONSART
                        (row.DNB_IDN, row.CREATOR, row.TITLE, year, PUB, row.SACHGRUPPE)
                            )
            except:
                err +=1

    start_Position = 1

conn.close()