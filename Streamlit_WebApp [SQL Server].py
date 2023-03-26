import pyodbc as odbc 
from typing import TypeVar, Generic

## Bibliotheken:
import base64
from datetime import datetime
import locale
import os
import json
import pickle
from sqlite3 import Cursor
import uuid
import re
import time
import math

import streamlit as st
import  streamlit_toggle as tog
import requests
from bs4 import BeautifulSoup
from lxml import etree
import pandas
import unicodedata
from time import sleep

backoff = 0
records_marc = ''

st.set_page_config(page_title='DNB Testabfrage')
if 'letsgo' not in st.session_state:
    st.session_state.letsgo = 0   



DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = '*****'
DATABASE_NAME = '*****'

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_Connection=yes;
"""
conn = odbc.connect(connection_string)

# #################################
# #Load From/To DB
# #################################
def executeSQL(conn, sqlQuery, parameter: TypeVar):
    cursor = conn.cursor()
    cursor.execute(sqlQuery,parameter)
    conn.commit()

def sqlToDataframe(conn, sqlQuery):
    cursor = conn.cursor()
    df = pandas.read_sql(sqlQuery, conn)
    return df.head(50000000000)

def readSQLTable(conn, sqlQuery, sortedColumnNumber):
    cursor = conn.cursor()
    df = pandas.read_sql(sqlQuery, conn)
    return sorted(df.values.tolist(), key=lambda x: x[sortedColumnNumber])

def load_SQLDataset():
    if(publicationen != "" and releasyear_start != ""):
        if(sachgruppen != ""):
            result = pandas.DataFrame(columns=['DNB_IDN', 'CREATOR', 'TITLE', 'ISBN', 'ERSCHEINUNGSJAHR', 'PUBLICATIONSART', 'SACHGRUPPE'])
            for sachgruppe in sgt:
                result = result.append(sqlToDataframe(conn,
                    ("Select DNB_IDN, CREATOR, TITLE, ISBN, ERSCHEINUNGSJAHR, PUBLICATIONSART, SACHGRUPPE "
                    "FROM DNB_DataSets "
                    "WHERE PUBLICATIONSART = '{}' " 
                    "AND SACHGRUPPE LIKE '%{}%' " 
                    "AND ERSCHEINUNGSJAHR BETWEEN {} AND {}".format(publicationen, sachgruppe, releasyear_start, releasyear_end))
                ), ignore_index=True)
            return result.drop_duplicates()
        elif(sachgruppen == ""):
            return sqlToDataframe(conn,
                (f"Select DNB_IDN, CREATOR, TITLE, ISBN, ERSCHEINUNGSJAHR, PUBLICATIONSART, SACHGRUPPE "
                "FROM DNB_DataSets "
                "WHERE PUBLICATIONSART = '{}'" 
                "AND ERSCHEINUNGSJAHR BETWEEN {} AND {}".format(publicationen, releasyear_start, releasyear_end))
            )
# #################################
# #Load From DNB
# #################################
def start_RequestDNB(start_position=1):   
    yearFilter = ''
    while True:  
        if(len(searchCount)):
            if(len(searchArray) == int(searchCount)):
                joinedStr=(' and ').join(searchArray)
                joinedSearchStr = 'woe='+joinedStr
        else:
            joinedSearchStr = ''
        sachgroupFilter = 'sgt=(' + sachgruppen +')'
        for x in range(releasyear_start, releasyear_end):
            yearFilter += "jhr=" + str(x) + " or "
        yearFilter += "jhr=" + str(releasyear_end)

        if (publicationen != "" and yearFilter != ""):
            if(joinedSearchStr != '' and sachgruppen != ''): 
                query = catalogFilter + " and " + yearFilter + " and " + sachgroupFilter + " and " + publicationsFilter + " and " + joinedSearchStr
            elif (publicationsFilter != "" and sachgruppen != ''):
                query = catalogFilter + " and " + yearFilter + " and " + sachgroupFilter + " and " + publicationsFilter
            elif (joinedSearchStr != ''):
                query = catalogFilter + " and " + yearFilter + " and " + publicationsFilter + " and " + joinedSearchStr
            else:
                query = catalogFilter + " and " + yearFilter + " and " + publicationsFilter
                
            parameter = {'version' : '1.1' , 'operation' : 'searchRetrieve' , 'query' : query, 'startRecord': start_position, 'recordSchema' : 'MARC21-xml', 
                        'maximumRecords': '100'} 
            try:
                r1 = requests.get("https://services.dnb.de/sru/dnb", params = parameter)  
                return r1   
            except:
                True
                # sleep(backoff)
                # backoff = (backoff*2) 
            
def metaDict_To_DataFrame():    
    result = [get_meta_dict(item) for item in records_marc]
    df = pandas.DataFrame(result).drop_duplicates(subset=['CREATOR', 'TITLE'])              
    return df

def get_meta_dict(item):

    ns = {"marc":"http://www.loc.gov/MARC21/slim"}
    xml = etree.fromstring(unicodedata.normalize("NFC", str(item)))
    
    #idn
    idn = xml.findall("marc:controlfield[@tag = '001']", namespaces=ns)
    try:
        idn = idn[0].text
    except:
        idn = '' 
        
    #creator
    creator1 = xml.findall("marc:datafield[@tag = '100']/marc:subfield[@code = 'a']", namespaces=ns)
    creator2 = xml.findall("marc:datafield[@tag = '110']/marc:subfield[@code = 'a']", namespaces=ns)
    subfield = xml.findall("marc:datafield[@tag = '110']/marc:subfield[@code = 'e']", namespaces=ns)
    
    if creator1:
        creator = creator1[0].text
    elif creator2:
        creator = creator2[0].text
        if subfield:
            creator = creator + " [" + subfield[0].text + "]"
    else:
        creator = ""
    
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
    date = xml.findall("marc:datafield[@tag = '264']/marc:subfield[@code = 'c']", namespaces=ns)
    try:
        date = re.findall(r"19[5-9][0-9]|20[0-2][0-9]", date[0].text)
        date = date[0]
    except:
        date = releasyear_end
    
        
    #ISBN
    isbn_new = xml.findall("marc:datafield[@tag = '020']/marc:subfield[@code = 'a']", namespaces=ns)
    isbn_old = xml.findall("marc:datafield[@tag = '024']/marc:subfield[@code = 'a']", namespaces=ns)
    if isbn_new:
        isbn = isbn_new[0].text
    elif isbn_old: 
        isbn = isbn_old[0].text
    else:    
        isbn = ''
      
    meta_dict = {
        "DNB_IDN":idn, 
        "CREATOR":creator, 
        "TITLE": titletext, 
        "ISBN":isbn, 
        "ERSCHEINUNGSJAHR":date, 
        "PUBLICATIONSART": publicationen,
        "SACHGRUPPE": '|'.join(sgt)
    }
    return meta_dict

def readSQL(conn, sqlQuery, parameter: TypeVar):
    sqlList = []
    cursor = conn.cursor()
    cursor.execute(sqlQuery,parameter)
    field_names = [i[0] for i in cursor.description]
    for field_index in range(0,len(field_names)):
        sqlList.append([item[field_index] for item in cursor.fetchall()])
    return sqlList
def readSQL(conn, sqlQuery):
    sqlList = []
    cursor = conn.cursor()
    cursor.execute(sqlQuery)
    field_names = [i[0] for i in cursor.description]
    for field_index in range(0,len(field_names)):
        sqlList.append([item[field_index] for item in cursor.fetchall()])
    return sqlList


# #################################
# #CSS-Style
# #################################

##Download Button
def download_button(object_to_download, download_filename, button_text, pickle_it=False):
    if pickle_it:
        try:
            object_to_download = pickle.dumps(object_to_download)
        except pickle.PicklingError as e:
            st.write(e)
            return None

    else:
        if isinstance(object_to_download, bytes):
            pass

        elif isinstance(object_to_download, pandas.DataFrame):
            object_to_download = object_to_download.to_csv(index=False)
            
        elif isinstance(object_to_download, pandas.ExcelFile):
            object_to_download = object_to_download.to_csv(index=False)

        # Try JSON encode for everything else
        else:
            object_to_download = json.dumps(object_to_download)

    try:
        # some strings <-> bytes conversions necessary here
        b64 = base64.b64encode(object_to_download.encode()).decode()

    except AttributeError as e:
        b64 = base64.b64encode(object_to_download).decode()

    button_uuid = str(uuid.uuid4()).replace('-', '')
    button_id = re.sub('\d+', '', button_uuid)

    custom_css = f""" 
        <style>
            #{button_id} {{
                background-color: rgb(19, 23, 32);
                color: inherit;
                padding: 0.25em 0.75em;
                position: relative;
                text-decoration: none;
                border-radius: 4px;
                border-width: 1px;
                border: 1px solid rgba(250, 250, 250, 0.2);
                border-image: initial;
            }} 
            #{button_id}:hover {{
                border-color: rgb(255, 75, 75);
                color: rgb(255, 75, 75);
            }}
            #{button_id}:active {{
                border-color: rgb(255, 75, 75);
                background-color: rgb(246, 51, 102);
                color: white;
                }}
        </style> """

    dl_link = custom_css + f'<a download="{download_filename}" id="{button_id}" href="data:file/txt;base64,{b64}">{button_text}</a>'

    return dl_link
    

##Header
def custom_css():
    return f""" 
        <style>
            #{"image"} {{
                width: 200px;
                height: 100px;
            }} 
            #{"divHeader"} {{
                background-color: transparent;
                height: 100px;
                border: 1px solid rgb(255, 75, 75,);
            }} 
            #{"imageAnd"} {{
                display: block;
                margin-left: auto;
                margin-right: auto;
                width: 100px;
            }} 
        </style> """
    

switchOffline = tog.st_toggle_switch(label='Load From Database', 
                        key="Key1", 
                        default_value=False, 
                        label_after = True, 
                        inactive_color = '#D3D3D3', 
                        active_color="#11567f", 
                        track_color="#29B5E8",
                        )
imageHTWG = custom_css() + f'<img src="{"https://cyberlago.net/wp-content/uploads/2020/03/HTWG_Markenzeichen_pos_1C.png"}" id="{"image"}" align="right">'
divHeader = custom_css() + f'<div id={"divHeader"}>' + imageHTWG +'</div>'
st.markdown(divHeader, unsafe_allow_html=True)


#Publikationsart
publication_settings = readSQLTable(conn, "Select * From Publicationen", 2)


publicationsFilter = ''
catalogFilter = 'catalog=dnb.hss'
publicationen = st.selectbox(
    'Publikationsart:',
    [item[0] for item in publication_settings])
for item in publication_settings:
    if publicationen == item[0]:
        publicationsFilter = item[1]

#Erscheinungsjahr
st.markdown("##### Bitte geben Sie das Erscheinungsjahr ein:")
releasyearOptions = [*range(2000, int(datetime.now().strftime('%Y'))+1, 1)] 
releasyear_start, releasyear_end = st.select_slider(
    'Erscheinungsjahr (Von - Bis)',
    options=releasyearOptions,
    value=(int(datetime.now().strftime('%Y'))-5, int(datetime.now().strftime('%Y'))))

#TODO
# Sachgruppe
# st.markdown("##### Bitte geben Sie die Sachgruppe ein:")
# sachgroup = st.text_input('Sachgruppe*:', placeholder="Bitte Sachgruppe eingeben")


opt_Fakultät = readSQLTable(conn, "Select Distinct Fakultät From Sachgruppen", 0)
st.markdown("##### Bitte wählen Sie die bevorzugten Fakultäten:")
option_Fakultät = st.multiselect(
    'Fakultät:',
    [item[0] for item in opt_Fakultät], [])

opt_Fächer = []
for item in option_Fakultät:
    if item in option_Fakultät:
        opt_sqlFächer = readSQLTable(conn, "Select Fachrichtung From Sachgruppen WHERE Fakultät = '{}'".format(item), 0)
        opt_Fächer+=[item[0] for item in opt_sqlFächer]
        opt_Fächer = list(set(opt_Fächer))

option_Fachrichtung = []
if(option_Fakultät != []):
    st.markdown("##### Bitte wählen Sie die bevorzugten Fachrichtungen:")
    option_Fachrichtung = st.multiselect(
        'Fachrichtung:', opt_Fächer, [])

sgt = []
for item in option_Fachrichtung:
    if(option_Fachrichtung != []):
        sgt_sql = readSQLTable(conn, "Select Sachgruppe From Sachgruppen WHERE Fachrichtung = '{}'".format(item), 0)
        sgt+=[item[0] for item in sgt_sql]
        sgt = list(set(sgt))
sachgruppen = " or ".join(sgt)
        


#Anzahl_Suchbegriffe: 
searchCount = ''
searchArray = []
if(switchOffline== False):
    st.markdown("##### Bitte geben Sie nun die Anzahl der Suchbegriff ein:")
    searchCount = st.text_input('Anzahl*:', placeholder="Bitte Anzahl eingeben") 
#Suchbegriffe
if searchCount.isnumeric():
    for x in range(int(searchCount)):
        searchArray.append(st.text_input('Suchbegriff eingeben:', placeholder="Bitte Suchbegriff eingeben", key="searchArray"+str(x)))
confirm = st.button('Los!', key='push')
if confirm:
    st.session_state.letsgo += 1  

if confirm and releasyear_start and switchOffline == False:
    #(Fist 100)
    info_time=st.info("Die Suche kann einige Zeit in Anspruch nehmen. Bitte warten Sie...", icon="ℹ️")
    progressbar_load = st.progress(0)
    r1 = start_RequestDNB()
    response = BeautifulSoup(r1.content)
    numRecords = int(response.find('numberofrecords').text) 
    st.write("Gefundene Treffer:", numRecords)
    records_marc = response.find_all('record', {'type':'Bibliographic'})
    #Next
    next_position = 101
    widget = st.empty()

    while (next_position <= numRecords):
        progressbar_load.progress(int(round((math.floor(next_position/100)/math.ceil(numRecords/100))*100,0)))
        
        r2 = start_RequestDNB(start_position=next_position)
        response = BeautifulSoup(r2.content)
        records_marcTMP = response.find_all('record', {'type':'Bibliographic'})
        records_marc+=records_marcTMP       
        next_position+= 100  
        widget.write(next_position)
    progressbar_load.progress(100)
    

           
if confirm and switchOffline == False:
    df = metaDict_To_DataFrame() 
    for index, row in df.iterrows():
        # executeSQL(conn,
        #     "DELETE FROM DNB_DataSets " +
        #     "WHERE DNB_IDN = ? ",
        #     row.DNB_IDN
        # )
        executeSQL(conn,
            "INSERT INTO DNB_DataSets " +
            "(DNB_IDN, CREATOR, TITLE, ISBN, ERSCHEINUNGSJAHR, PUBLICATIONSART, SACHGRUPPE, Abrufdatum) "+ 
            "values(?,?,?,?,?,?,?, FORMAT(getdate(),'dd.MM.yyyy'))",
            (row.DNB_IDN, row.CREATOR, row.TITLE, row.ISBN, row.ERSCHEINUNGSJAHR, row.PUBLICATIONSART, row.SACHGRUPPE)
        )

#     st.info("Ihre Daten wurden in die Datenbank imporiert", icon="ℹ️")
#     st.markdown("##### Download:") 

#     with st.expander("Download Anzeigen",False):   
#         st.markdown("<br />"+
#             download_button(df.to_csv().encode('utf-8') , 'DNB_Export.csv', 'Download CSV')+
#             download_button(df.to_html().encode('utf-8'), 'DNB_Export.html', 'Download HTML')+
#             "<br /><br />", unsafe_allow_html=True)
            
        
        
#     st.markdown("##### Darstellung als Tabelle:")
#     with st.expander("Tabelle Anzeigen",False):
#         st.dataframe(df)


# elif confirm and switchOffline == True:
#     df = load_SQLDataset() 

#     st.markdown("##### Download:") 

#     with st.expander("Download Anzeigen",False):   
#         st.markdown("<br />"+
#             download_button(df.to_csv().encode('utf-8') , 'DNB_Export.csv', 'Download CSV')+
#             download_button(df.to_html().encode('utf-8'), 'DNB_Export.html', 'Download HTML')+
#             "<br /><br />", unsafe_allow_html=True)
            
        
        
#     st.markdown("##### Darstellung als Tabelle:")
#     with st.expander("Tabelle Anzeigen",False):
#         st.dataframe(df)
# else:
#     st.write('')
    
# st.write(" ")
           
# footer="""<style>
#         .footer {
# left: 0;
# bottom: 0;
# width: 100%;
# text-align: center;
# }
# </style>
# <div class="footer">
# <br><br><br>
# </div>
# """

# st.markdown(footer,unsafe_allow_html=True)

# conn.close()