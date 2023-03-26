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
import base64

import pymongo

client = pymongo.MongoClient("*****")
db = client["dnb"]
collection = db.dnb_collection

#################################
#Streamlit-Setup
#################################

st.set_page_config(page_title='DNB Testabfrage')
if 'letsgo' not in st.session_state:
    st.session_state.letsgo = 0   
if 'downclick' not in st.session_state:
    st.session_state.downclick = 0
    
def downclick(): 
    st.session_state.downclick += 1
#################################





#################################
#CSS Style + Html
#################################

#Download Button
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

#Header
custom_css = f""" 
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

imageHTWG = custom_css + f'<img src="{"https://cyberlago.net/wp-content/uploads/2020/03/HTWG_Markenzeichen_pos_1C.png"}" id="{"image"}" align="right">'
#imageDNB = custom_css + f'<img src="{"https://files.dnb.de/DFG-Viewer/DNB-Logo-Viewer.jpg"}" id="{"image"}" align="left">'
#imageAND = custom_css + f'<img src="{"https://static.thenounproject.com/png/3885658-200.png"}" id="{"imageAnd"}" align="center">'
divHeader = custom_css + f'<div id={"divHeader"}>' + imageHTWG +'</div>'
switchText = 'Online-Modus'

switch = tog.st_toggle_switch(label='Offline-Modus', 
                    key="Key1", 
                    default_value=False, 
                    label_after = True, 
                    inactive_color = '#D3D3D3', 
                    active_color="#11567f", 
                    track_color="#29B5E8",
                    )

st.markdown(divHeader, unsafe_allow_html=True)

#################################





#################################
#Streamlit Elemente
#################################

#Format
st.markdown("##### Bitte wählen Sie das Metadatenformat für die Ausgabe:")
meta = st.selectbox(
        'Metadatenformat:',
        ('MARC21-xml', 'DNB Casual (oai_dc)', 'RDF (RDFxml)'))
if meta == "DNB Casual (oai_dc)":
    dataform = "oai_dc"
elif meta == "RDF (RDFxml)":
    dataform = "RDFxml"
elif meta == "MARC21-xml":
    dataform = "MARC21-xml"
else: 
    dataform = ""

#Publikation
pupbicationen = st.selectbox(
        'Publikation:',
        ('Dissertationen & Habilitationen', 'Dissertationen', 'Habilitationen', 'Alle Hochschulpublikationen'))
if pupbicationen == "Dissertationen & Habilitationen":
    publicationsFilter = "(woe=disser* or woe=habil*)"
elif pupbicationen == "Dissertationen":
    publicationsFilter = "woe=disser*"
elif pupbicationen == "Habilitationen":
    publicationsFilter = "woe=habil*"
else: 
    publicationsFilter = ""

#Erscheinungsjahr
st.markdown("##### Bitte geben Sie das Erscheinungsjahr ein:")
# releasyear = st.text_input('Erscheinungsjahr:', placeholder="Bitte Erscheinungsjahr eingeben")

releasyearOptions = [*range(2000, int(datetime.now().strftime('%Y'))+1, 1)] 
releasyear_start, releasyear_end = st.select_slider(
    'Erscheinungsjahr (Von - Bis)',
    options=releasyearOptions,
    value=(int(datetime.now().strftime('%Y'))-5, int(datetime.now().strftime('%Y'))))


#Sachgruppe
sachgroup = ""
st.markdown("##### Bitte geben Sie die Sachgruppe ein:")
sachgroup = st.text_input('Sachgruppe*:', placeholder="Bitte Sachgruppe eingeben")

#Anzahl_Suchbegriffe: 
searchCount = ''
if(switch== False):
    st.markdown("##### Bitte geben Sie nun die Anzahl der Suchbegriff ein:")
    searchCount = st.text_input('Anzahl*:', placeholder="Bitte Anzahl eingeben") 

#Suchbegriffe
searchArray = []
if searchCount.isnumeric():
    for x in range(int(searchCount)):
        searchArray.append(st.text_input('Suchbegriff eingeben:', placeholder="Bitte Suchbegriff eingeben", key="searchArray"+str(x)))
confirm = st.button('Los!', key='push')
if confirm:
    st.session_state.letsgo += 1 
#################################   
 
 
 
 
    
## TEIL 1 -------------------------------------------------------------------------------------------  
    
#################################
#Suche ausführen
#################################

def enquiryDB():

    st.write(pupbicationen)
    if(pupbicationen != "" and releasyear_start != "" and sachgroup != ""):
        result = db.dnb_collection.find({"DATE": { "$gte" : str(releasyear_start), "$lte": str(releasyear_end)}, 'SACHGRUPPE': sachgroup, 'PUBLICATION': pupbicationen})
        result = db.dnb_collection.find({"DATE": { "$gte" : str(releasyear_start), "$lte": str(releasyear_end)}, 'SACHGRUPPE': sachgroup, 'PUBLICATION': pupbicationen})
    elif(pupbicationen != "" and releasyear_start != "" and sachgroup == ""):
        result = db.dnb_collection.find({"DATE": { "$gte" : str(releasyear_start), "$lte": str(releasyear_end)}, 'PUBLICATION': pupbicationen})
    elif(pupbicationen != "" and releasyear_start == "" and sachgroup != ""):
        result = db.dnb_collection.find({'SACHGRUPPE': sachgroup, 'PUBLICATION': pupbicationen}) 
    elif(pupbicationen == "" and releasyear_start != "" and sachgroup != ""):
        result = db.dnb_collection.find({"DATE": { "$gte" : str(releasyear_start), "$lte": str(releasyear_end)}, 'SACHGRUPPE': sachgroup})
    return pandas.DataFrame(list(result))

#Suchfunktion
def enquiryDNB(start_position=1):   
    backoff = 1
    sachgroupFilter = ''
    while True:  
        if(len(searchCount)):
            if(len(searchArray) == int(searchCount)):
                joinedStr=(' and ').join(searchArray)
                joinedSearchStr = 'woe='+joinedStr
        else:
            joinedSearchStr = ''
        sachgroupFilter = 'sgt=' + sachgroup
        yearFilter = ""
        for x in range(releasyear_start, releasyear_end):
            yearFilter += "jhr=" + str(x) + " or "
        yearFilter += "jhr=" + str(releasyear_end)
        catalogFilter = 'catalog=dnb.hss'

        if(publicationsFilter != "" and joinedSearchStr != '' and sachgroup != ''): 
            query = catalogFilter + " and " + yearFilter + " and " + sachgroupFilter + " and " + publicationsFilter + " and " + joinedSearchStr
        elif (publicationsFilter != "" and sachgroup != ''):
            query = catalogFilter + " and " + yearFilter + " and " + sachgroupFilter + " and " + publicationsFilter
        elif (publicationsFilter != "" and sachgroup != ''):
            query = catalogFilter + " and " + yearFilter + " and " + publicationsFilter + " and " + joinedSearchStr
        elif (publicationsFilter != ""):
            query = catalogFilter + " and " + yearFilter + " and " + publicationsFilter
        elif(publicationsFilter == "" and joinedSearchStr != '' and sachgroup != ''):
            query = catalogFilter + " and " + yearFilter + " and " + joinedSearchStr + " and " + sachgroupFilter
        elif(publicationsFilter == "" and sachgroup != ''):
            query = catalogFilter + " and " + yearFilter + " and " + sachgroup         
        elif(publicationsFilter == "" and joinedSearchStr != ''):
            query = catalogFilter + " and " + yearFilter + " and " + joinedSearchStr
        else: 
            query = catalogFilter + " and " + yearFilter
            
        parameter = {'version' : '1.1' , 'operation' : 'searchRetrieve' , 'query' : query, 'startRecord': start_position, 'recordSchema' : dataform, 
                    'maximumRecords': '100'} 
        try:
            r1 = requests.get("https://services.dnb.de/sru/dnb", params = parameter)  
            return r1   
        except:
            sleep(backoff) #Wegen Wiederholungsfehler Einbau von BackOff
            backoff = (backoff*2) 
            

if confirm and releasyear_start and switch == False:
    t0 = time.time() #Zeitmessung starten
    #Ausführen der ersten Sucheanfrage (Fist 100)
    info_time=st.info("Die Suche kann einige Zeit in Anspruch nehmen. Bitte warten Sie...", icon="ℹ️")
    progressbar_load = st.progress(0)
    r1 = enquiryDNB()
    response = BeautifulSoup(r1.content)
    numRecords = int(response.find('numberofrecords').text) 
    st.write("Gefundene Treffer:", numRecords)
    records_oai_rdf = response.find_all('record')
    records_marc = response.find_all('record', {'type':'Bibliographic'})
    
    next_position = 101
    while (next_position <= numRecords):
        progressbar_load.progress(int(round((math.floor(next_position/100)/math.ceil(numRecords/100))*100,0)))
        
        r2 = enquiryDNB(start_position=next_position)
        response = BeautifulSoup(r2.content)
        records_marcTMP = response.find_all('record', {'type':'Bibliographic'})
        records_oai_rdfTMP = response.find_all('record')
        records_oai_rdf+=records_oai_rdfTMP
        records_marc+=records_marcTMP       
        next_position+= 100
    
    progressbar_load.progress(100)
    t1 = time.time() #Zeitmessung Ende
    info_time.info('Vergangene Zeit in Sekungen: ' + str(int(t1-t0)) + 'Sekunden') 
    
#################################





## TEIL 2 -------------------------------------------------------------------------------------------

#################################
#Funktionen für Zuordnen der XML-Felder
#################################

#Funktion für Titeldaten in OAI-DC
def parse_record_dc(record):
    
    ns = {"dc": "http://purl.org/dc/elements/1.1/", 
          "xsi": "http://www.w3.org/2001/XMLSchema-instance"}
    xml = etree.fromstring(unicodedata.normalize("NFC", str(record)))
    
    #idn
    idn = xml.xpath(".//dc:identifier[@xsi:type='dnb:IDN']", namespaces=ns) #--> Adressiert das Element direkt   
    try:
        idn = idn[0].text
    except:
        idn = ''
    
    #creator:
    creator = xml.xpath('.//dc:creator', namespaces=ns)
    try:
        creator = creator[0].text
    except:
        creator = ""
    
    #titel
    titel = xml.xpath('.//dc:title', namespaces=ns)
    try:
        titel = titel[0].text
    except:
        titel = ""
        
    #date
    date = xml.xpath('.//dc:date', namespaces=ns)
    try:
        locale.setlocale(locale.LC_ALL, 'de_DE')
        date1 = datetime.strptime(date[0].text , '%d.%m.%Y').strftime('%Y')
    except:
        date1 = ""
    try:
        locale.setlocale(locale.LC_ALL, 'de_DE')
        date2 = datetime.strptime(date[0].text , '%Y').strftime('%Y')
    except:
        date2 = ""    
    
    if(date1 != "") :
        date = date1
    else:
        date = date2
    
    #publisher
    publ = xml.xpath('.//dc:publisher', namespaces=ns)
    try:
        publ = publ[0].text
    except:
        publ = ""
     
    #identifier
    ids = xml.xpath('.//dc:identifier[@xsi:type="tel:ISBN"]', namespaces=ns)
    try:
        ids = ids[0].text
    except:
        ids = ""
        
    #urn
    urn = xml.xpath('.//dc:identifier[@xsi:type="tel:URN"]', namespaces=ns)
    try:
        urn = urn[0].text
    except:
        urn = ""
         
    meta_dict = {
        "DNB_IDN":idn, 
        "CREATOR":creator, 
        "TITLE":titel, 
        "DATE":date, 
        "PUBLISHER":publ, 
        "URN":urn, 
        "ISBN":ids, 
        "ABFRAGEDATUM": datetime.now().strftime('%d.%m.%Y'),
        "PUBLICATION": pupbicationen,
        "SACHGRUPPE": sachgroup
    }
    return meta_dict
                 
#Function für Titeldaten in MARC21
def parse_record_marc(item):

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
        locale.setlocale(locale.LC_ALL, 'de_DE')
        date1 = datetime.strptime(date[0].text , '%d.%m.%Y').strftime('%Y')
    except:
        date1 = ""
    try:
        locale.setlocale(locale.LC_ALL, 'de_DE')
        date2 = datetime.strptime(date[0].text , '%Y').strftime('%Y')
    except:
        date2 = ""  
    
    if(date1 != "") :
        date = date1
    elif(date2 != ""):
        date = date2
    elif(len(date) > 0):
        date = date[0].text
    else:
        date = ""
    
    #publisher
    publ = xml.findall("marc:datafield[@tag = '264']/marc:subfield[@code = 'b']", namespaces=ns)
    try:
        publ = publ[0].text
    except:    
        publ = ''
        
    #URN
    testurn = xml.findall("marc:datafield[@tag = '856']/marc:subfield[@code = 'x']", namespaces=ns)
    urn = xml.findall("marc:datafield[@tag = '856']/marc:subfield[@code = 'u']", namespaces=ns)
    
    if testurn:
        urn = urn[0].text
    else:    
        urn = ''
          
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
        "PUBLISHER":publ, 
        "URN":urn,   
        "ISBN":isbn, 
        "DATE":date, 
        "ABFRAGEDATUM": datetime.now().strftime('%d.%m.%Y'),
        "PUBLICATION": pupbicationen,
        "SACHGRUPPE": sachgroup
    }
    return meta_dict
       
#Funktion für Titeldaten in RDF:
def parse_record_rdf(record):
    
    ns = {"xlmns":"http://www.loc.gov/zing/srw/", 
          "agrelon":"https://d-nb.info/standards/elementset/agrelon#",
          "bflc":"http://id.loc.gov/ontologies/bflc/",
          "rdau":"http://rdaregistry.info/Elements/u/",
          "dc":"http://purl.org/dc/elements/1.1/",
          "rdau":"http://rdaregistry.info/Elements/u",
          "bibo":"http://purl.org/ontology/bibo/",
          "dbp":"http://dbpedia.org/property/",  
          "dcmitype":"http://purl.org/dc/dcmitype/", 
          "dcterms":"http://purl.org/dc/terms/", 
          "dnb_intern":"http://dnb.de/", 
          "dnbt":"https://d-nb.info/standards/elementset/dnb#", 
          "ebu":"http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#", 
          "editeur":"https://ns.editeur.org/thema/", 
          "foaf":"http://xmlns.com/foaf/0.1/", 
          "gbv":"http://purl.org/ontology/gbv/", 
          "geo":"http://www.opengis.net/ont/geosparql#", 
          "gndo":"https://d-nb.info/standards/elementset/gnd#", 
          "isbd":"http://iflastandards.info/ns/isbd/elements/", 
          "lib":"http://purl.org/library/", 
          "madsrdf":"http://www.loc.gov/mads/rdf/v1#", 
          "marcrole":"http://id.loc.gov/vocabulary/relators/",
          "mo":"http://purl.org/ontology/mo/", 
          "owl":"http://www.w3.org/2002/07/owl#", 
          "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
          "rdfs":"http://www.w3.org/2000/01/rdf-schema#", 
          "schema":"http://schema.org/", 
          "sf":"http://www.opengis.net/ont/sf#", 
          "skos":"http://www.w3.org/2004/02/skos/core#", 
          "umbel":"http://umbel.org/umbel#", 
          "v":"http://www.w3.org/2006/vcard/ns#", 
          "vivo":"http://vivoweb.org/ontology/core#", 
          "wdrs":"http://www.w3.org/2007/05/powder-s#", 
          "xsd":"http://www.w3.org/2001/XMLSchema#"}
    
    xml = etree.fromstring(unicodedata.normalize("NFC", str(record)))
   
    #idn
    idn = xml.findall(".//dc:identifier", namespaces=ns)
    try:
        idn = idn[0].text
    except:
        idn = '' 
        
    #creator
    creator = record.find_all('rdau:p60327')
    
    try:
        creator = creator[0].text
    except:
        creator = ""
        
    #title
    test = record.find_all('dc:title')
    
    try:
        test = test[0].text
    except:
        test = ""
        
    #date
    date = record.find_all('dcterms:issued')
    try:
        locale.setlocale(locale.LC_ALL, 'de_DE')
        date1 = datetime.strptime(date[0].text , '%d.%m.%Y').strftime('%Y')
    except:
        date1 = ""
    try:
        locale.setlocale(locale.LC_ALL, 'de_DE')
        date2 = datetime.strptime(date[0].text , '%Y').strftime('%Y')
    except:
        date2 = ""    
    
    if(date1 != "") :
        date = date1
    else:
        date = date2
      
    #publisher
    publ = record.find_all('dc:publisher')
    try:
        publ = publ[0].text
    except:
        publ = ""    
    
    #urn
    urn = record.find_all('umbel:islike')
    try:
        urn = urn[0]
        urn = urn.get('rdf:resource')
    except:
        urn = ""
    
    #isbn
    isbn = xml.findall(".//bibo:isbn13", namespaces=ns)
    isbn10 = xml.findall(".//bibo:isbn10", namespaces=ns)
    
    if isbn:
        isbn = isbn[0].text
    elif isbn10: 
        isbn = isbn10[0].text
    else:
        isbn = ""
     
    meta_dict = {
        "DNB_IDN":idn, 
        "CREATOR":creator, 
        "TITLE":test, 
        "PUBLISHER":publ, 
        "URN":urn, 
        "ISBN":isbn, 
        "DATE":date, 
        "ABFRAGEDATUM": datetime.now().strftime('%d.%m.%Y'),
        "PUBLICATION": pupbicationen,
        "SACHGRUPPE": sachgroup
    }
    return meta_dict




#################################
#Funktion zur XML->Dataframe 
#################################
         
def table():    
    if dataform == "oai_dc":
        result = [parse_record_dc(record) for record in records_oai_rdf]    
        df = pandas.DataFrame(result).drop_duplicates(subset=['CREATOR', 'TITLE'])
    elif dataform == "MARC21-xml":
        result2 = [parse_record_marc(item) for item in records_marc]
        df = pandas.DataFrame(result2).drop_duplicates(subset=['CREATOR', 'TITLE'])      
    elif dataform == "RDFxml":
        result3 = [parse_record_rdf(item) for item in records_oai_rdf]
        df = pandas.DataFrame(result3).drop_duplicates(subset=['CREATOR', 'TITLE'])       
    else:
        st.write("Es wurde noch keine Suchanfrage gestellt.")
        
    return df





#################################
#Ausgabe der Ergebnisse
#Bereitstellung Download XML,CSV
#################################
              
if confirm:
    st.markdown("##### Download:")


    def handle_import(args):
       for item in df.to_dict('records'):
                    if db.mycollection.find({'DNB_IDN': item.get('DNB_IDN')}):
                        db.dnb_collection.delete_many({'DNB_IDN': item.get('DNB_IDN')})
                    db.dnb_collection.insert_one(item)
                    st.write('OK')
    if(switch == True):
        df = enquiryDB()
    else:
        df = table()
        st.button('In Datenbank importieren', on_click=handle_import, args=('123',))
    

    

    with st.expander("Download Anzeigen",False):  
        json_btn = ""
        xml_btn = ""
        if(switch == True):
            json_btn = ""
            xml_btn = ""
        else:
            json_btn = download_button(df.to_json(orient="records").encode('utf-8') , 'DNB_Export.json', 'Download JSON')
            xml_btn = download_button(df.to_xml().encode('utf-8'), 'DNB_Export.xml', 'Download XML')

        st.markdown("<br />"+
            xml_btn+
            json_btn +
            download_button(df.to_csv().encode('utf-8') , 'DNB_Export.csv', 'Download CSV')+
            download_button(df.to_html().encode('utf-8'), 'DNB_Export.html', 'Download HTML')+
            "<br /><br />", unsafe_allow_html=True)
            
        
        
    st.markdown("##### Darstellung als Tabelle:")
    with st.expander("Tabelle Anzeigen",False):
        st.dataframe(df)
else:
    st.write(" ")
    
st.write(" ")
    
if st.session_state.downclick != 0:
    st.write("Button wurde bereits geklickt")
    st.session_state["df"]
           
footer="""<style>
        .footer {
left: 0;
bottom: 0;
width: 100%;
text-align: center;
}
</style>
<div class="footer">
<br><br><br>
</div>
"""

st.markdown(footer,unsafe_allow_html=True)