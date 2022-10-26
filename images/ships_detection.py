from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import requests
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
import csv
import base64
# import datetime
import shapely
from shapely import wkt, geometry
from shapely.geometry.multipolygon import MultiPolygon
#from shapely.geometry import shape, polygon
from collections.abc import Iterable
from xml.etree import ElementTree
from IPython.display import clear_output
import geojson
import json
import http.client
import ssl
import sys
import shutil
import os
from datetime import datetime, timedelta
requests.packages.urllib3.disable_warnings()

""" 
work_dir = os.path.dirname(os.path.realpath(__file__))
utils_dir = 'utils'
fix_dir = 's2cloudless_fix'
fix_file = 'S2PixelCloudDetector.py'
fix_file_path = os.path.join(work_dir,utils_dir,fix_dir,fix_file)
fix_file_path
shutil.copy(fix_file_path, '/opt/conda/envs/race/lib/python3.7/site-packages/s2cloudless')
"""

from cloud_coverage.detector import cloud_detection

########################################## PKG IMPORT ############################################################

# conn = http.client.HTTPSConnection("pygeoapi-eoxhub.98373457-4e7e-4c85-8bb2-b806251a16de.hub.eox.at", context = ssl._create_unverified_context())
""" 
date_from = sys.argv[1]
date_to = sys.argv[2]
print('Start date: ', date_from)
print('End date: ', date_to)
"""

################################## Manual ops #############################################
""" 
user_selection = False
ingestion_flag = None
database = None

while not user_selection:
    user_selection = input("Processing for ingestion into geoDB? y [yes] n [no]: ")
    #print('User selected option: ', type(user_selection))
    if user_selection == 'y':
        ingestion_flag = True
        user_selection = True
        selected_db = input("Enter db: 1 [production db] 2 [test db]: ")
        if selected_db == '1':
            database = 'eodash'
        if selected_db == '2':
            database = 'eodash_stage'
        break
    if user_selection == 'n':
        ingestion_flag = False
        user_selection = True
        break
    else:
        user_selection = False


print('Ingestion mode: ', ingestion_flag)
if ingestion_flag:
    if database == 'eodash':
        print('Production db selected.')
    else:
        print('Test db selected.')
"""
####################################### end manual ops ################################

database = 'eodash_stage'
ingestion_flag = True

date_from = str((datetime.today() - timedelta(days=7)).date())
date_to = str((datetime.today() - timedelta(days=1)).date())

#set variables
client_id = "890744f6-6cef-4c1f-b436-a054bbbc7b9c"
client_secret = "h8~F-GgLa.e5PNo+i.x_:<8}BHgkqj1%qyA5Ei#2"
PLANET_API_KEY = 'ebe85cda11034da9a47914b29665ca5c'
Planet_collection_id = "DSS10-f4a28a33-9b73-4edb-9a40-f20cc0d45106"
Pleiades_collection_id = "DSS10-3c4daecf-09f3-451c-8c3c-90e356cbd673"
Pleiades_2_8_collection_id = "DSS10-67d461fe-6a53-48f3-b9e0-e5b49dd3c93e"
New_Pleiades_collection_id = "DSS10-649d1c03-c071-46d1-b79b-4a1b4ff70fd9"
New_Pleiades_COVID_collection_id = "DSS10-9a5fc288-6d70-4ca3-aeb6-bf7a22baa5e8"
New_Pleaiades_2_8_collection_id = "DSS10-02166702-25cc-426b-a9e8-f18c2d05ec28"
New_PlanetScope_collection_id = "DSS10-9a5fc288-6d70-4ca3-aeb6-bf7a22baa5e8"
SENTINEL_2_L1C_collection_id = "DSS1" 
SENTINEL_2_L2A_collection_id = "DSS2"
SENTINEL_1_collection_id = "DSS3"

#obtain the token (not sure that it is needed in this NB)

def get_token (client_id, client_secret):
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url='https://services.sentinel-hub.com/oauth/token',
                              client_id=client_id, client_secret=client_secret)
    resp = oauth.get("https://services.sentinel-hub.com/oauth/tokeninfo")

    return oauth
oauth = get_token (client_id, client_secret)
# print(oauth.token)

payload = "{\n    \"inputs\":[\n        {\n            \"id\": \"notebook\",\n            \"value\": \"ship/RACE_ships.ipynb\"\n        },\n        {\n            \"id\": \"parameters\",\n            \"value\": \"WPS_PARAMETERS\"\n        },\n         {\n              \"id\": \"cpu_requests\",\n              \"value\": \"0.2\"\n          },\n          {\n              \"id\": \"cpu_limit\",\n              \"value\": \"0.6\"\n          },\n          {\n              \"id\": \"mem_requests\",\n              \"value\": \"1.9Gi\"\n          },\n          {\n              \"id\": \"mem_limit\",\n              \"value\": \"2.6Gi\"\n          }\n    ]\n}" 


status_url = "https://pygeoapi-eoxhub.98373457-4e7e-4c85-8bb2-b806251a16de.hub.eox.at/processes/execute-notebook/jobs/SHIP_JOB_ID"

def get_job_status(job_url):
    payload = {}
    headers = {}
    response = requests.request("GET", job_url, headers=headers, data=payload, verify=False) 

    try:
        out = json.loads(response.text)
        job_status = out["status"]
    except:
        job_status='accepted'
    return job_status


headers = {
  'Content-Type': 'application/json'
}


def invoke_wps(body, headers):
    url = "https://pygeoapi-eoxhub.98373457-4e7e-4c85-8bb2-b806251a16de.hub.eox.at/processes/execute-notebook/jobs?async-execute=True"

    try:
        response = requests.request("POST", url, headers=headers, data=body, verify=False)
        loc_header = response.headers.get('Location')
    except requests.exceptions.HTTPError as e:
        print(f"Error {e.response.content}")
    return loc_header

def download_results (filename, outputpath):
    download_url = f"https://edc-98373457-4e7e-4c85-8bb2-b806251a16de.s3.eu-central-1.amazonaws.com/{filename}"
    print (f"Downloading {download_url}")
    try:
        response = requests.get(download_url, verify=False)
        response.raise_for_status()                        
        open(f"{outputpath}{filename}", 'wb').write(response.content)
    except requests.exceptions.HTTPError as e:
        print (f"Error {e.response.content}")  
		

# aoisfile = "./Inputs/ships/Ships_Input_for_WPS_call_Suez.csv"
aoisfile = "./Inputs/ships/Ships_Input_for_WPS_call.csv"

print('\nAoIs input file: ', aoisfile)
# input('check input file')


# date_from = '2019-08-01'
# date_to = '2019-08-31'

sensor = 'sentinel2'#planetScope pleiadesCovid pleiades pleiadesCovid28 sentinel2
indicator = 'E200'
overlap_perc = 80 

CLOUD_TH = 20.
cloud_filter = True
cloud_sensitivity = 0.45
debug = False    # True for annotations, False otherwise




today = datetime.today().strftime('%Y%m%d')
current_time = datetime.now().time()
proc_time = current_time.strftime("T%H%M%S")

date_from_str = date_from.replace("-","")
date_to_str = date_to.replace("-","")

if debug is True:
    outdatafolder = f"./Outputs/{indicator}/Debug/{today}{proc_time}_{date_from_str}-{date_to_str}/"
    outcsvfolder = f"./Outputs/{indicator}/Debug/{today}{proc_time}_{date_from_str}-{date_to_str}/csv/"
    # extensions = {"_masked.png", ".xml"}
else:
    outdatafolder = f"./Outputs/{indicator}/{today}{proc_time}_{date_from_str}-{date_to_str}/"
    outcsvfolder = f"./Outputs/{indicator}/{today}{proc_time}_{date_from_str}-{date_to_str}/csv/"
    # extensions = {"_masked_bbox.tif", "_masked_bbox.png", ".geojson"}

print(outdatafolder)
print(outcsvfolder)

if not os.path.exists(outdatafolder):
    print('creating out data folder')	
    os.makedirs(outdatafolder)

if not os.path.exists(outcsvfolder):
    print('create out csv folder')
    os.makedirs(outcsvfolder)

print("Current working directory: ", os.getcwd())

oauth = get_token (client_id, client_secret)

#the collectionid is set in the query below

#today = datetime.date.today().strftime('%Y%m%d')
#current_time = datetime.datetime.now().time()
#proc_time = current_time.strftime("T%H%M%S")

oauth = get_token (client_id, client_secret)
jobs=[]

basesearch_url = f"https://creodias.sentinel-hub.com/ogc/wfs/e85faeeb-72ce-4de6-984c-c6dd0ede3ee0?"
print('\nFiltering dates according to cloudiness over AoI...\n')
with open(aoisfile, "r") as f:
    csv_reader = csv.reader(f, delimiter=",")
    for i,line in enumerate(csv_reader):
        count = 0
        if i == 0:
            continue
        dates=[]
        indicator_from_file = line[2]
        if indicator == indicator_from_file:
            aoiid = line[1]
            #print(line[1])

            pol = wkt.loads(str(line[3]))
            pol_subAOI = wkt.loads(str(line[4]))
            #Multipolyg to Polyg
            if isinstance(pol, Iterable):
                polyg = list(pol)[0].wkt
            else:
                polyg = pol.wkt
            g1 = wkt.loads(polyg)
            geom1 = geometry.shape(g1)
            offset=0
   
            multi_poly = str(MultiPolygon([geom1]))
            print('AoI id:', aoiid)
            if aoiid == 'EG1':
                overlap_perc = 30
                cloud_sensitivity = .1
                #print('aoi id found:', aoiid)
                #print('overlap percentage:', overlap_perc)
                #print('cloud sensitivity:', cloud_sensitivity)
                # input('check id')
            filtered_dates = cloud_detection.detect(date_from, date_to, str(pol_subAOI), CLOUD_TH, cloud_sensitivity)
            print('\nFiltered date-time for {} from {} to {}:\n{}'.format(aoiid, date_from, date_to, filtered_dates))
            for i in range(len(filtered_dates)):
                fromdate = filtered_dates[i].split('T')[0]+'T00:00:00'
                todate = filtered_dates[i].split('T')[0]+'T23:59:59'
                # print(fromdate)
                # print(todate)
                offset = 0
                
                while True:
                    #print (offset)
                    search_url = f"{basesearch_url}\
service=WFS&version=1.0.0&request=GetFeature&\
typenames={SENTINEL_2_L2A_collection_id}&\
outputformat=application/json&\
FEATURE_OFFSET={offset}&\
GEOMETRY={polyg}&\
time={fromdate}/{todate}&srsname=EPSG:4326&\
maxcc=100"
                    print('Search url:\n',search_url)
                    try:
                        response = oauth.get(search_url)
                        response.raise_for_status()
                        results = response.json()
                    except requests.exceptions.HTTPError as e:
                        print (f"Error {e.response.content}")
                        break      
                    feats = len(results['features'])
                    #print(feats)
                    if feats == 0:
                        break
                    #pagination
                    count = count+feats
                    offset = offset+100
                    for subfeature in results['features']:
                        geom2 = geometry.shape(subfeature['geometry'])
                        overlap = geom1.intersection(geom2).area/geom1.area*100
                        ftime_out = subfeature['properties']['time']
                        
                        print('overlap: ', overlap)
                        if overlap >= overlap_perc:
                            fdate = subfeature['properties']['date']
                            #print (fdate)
                            # print('aoiid: ', aoiid)
                            # print('fdate: ', fdate)
                            # print('dates: ', dates)
                            # input('stop')
                            if fdate in dates and aoiid != "EG1" and aoiid != "EG2":
                                print ("Found duplicate")
                                continue
                            dates.append(fdate)
                            #fdate = '2020-05-09'
                            ftime = subfeature['properties']['time']
                            # print('ftime: ', ftime)
                            # input('check ftime')
                            #ftime = '10:35:59'
                            #time_obj = datetime.datetime.strptime(ftime,'%H:%M:%S')
                            #filenametime = datetime.datetime.strftime(time_obj,'%H:%M')
                            outputfile = f"{indicator}_S2_PLES_{today}{proc_time}.csv"
                            parameters = f'INDICATOR: \'{indicator}\'\n\
AOIID: \'{aoiid}\'\n\
SENSOR: \'{sensor}\'\n\
DATE: \'{fdate}T{ftime}\'\n\
OUTPUT_NAME: \'{outputfile}\'\n\
CLOUD_FILTER: {cloud_filter}\n\
CLOUD_SENSITIVITY: \'{cloud_sensitivity}\'\n\
DEBUG: {debug}'
    #DEBUG: '+str(debug).lower()
                            # print(parameters)
                            # input('check params')
                            #continue
                            message_bytes = parameters.encode('ascii')
                            base64_bytes = base64.b64encode(message_bytes)
                            base64_message = base64_bytes.decode('ascii')
                            # print('type base64 msg: ', type(base64_message))    # <class 'str>
                            # input('waii base64 msg type')
                            location_header = invoke_wps(payload.replace('WPS_PARAMETERS', base64_message), headers)
                            jobId = location_header.rsplit('/', 1)[-1]
                            # print(jobId)
                            #input('check jobID')
                            # wps_response = invoke_wps(payload.replace('PARAMETERS', base64_message))
                            # print(wps_response)
                            # print('decoded response: ', wps_response.decode('UTF-8'))
                            # print('xml string:  ', wps_response.content)
                            # input('stop stop stop')
                            # parser = ElementTree.XMLParser(encoding="utf-8")
                            # root = ElementTree.fromstring(wps_response.content)
                            # jobId = root[0].text
                            # print (jobId)
                            filebasename = f"{indicator_from_file}_{aoiid}_{fdate.replace('-','')}T{ftime.replace(':','')}"
                            #filebasename = f"{indicator_from_file}_{aoiid}_{fdate.replace('-','')}T{filenametime.replace(':','')}"    
                            job={}
                            job["jobId"] = jobId
                            job["filebasename"] = filebasename
                            jobs.append(job)
                            #jobs.append(jobId)
                            #break #(5 request)
                #print('\nDates from search url:\n', dates)    
                #break #(single request)

print('\nDates from search url:\n', dates)
print('\nPerforming ships detection...\n')
#poll status and download results
import time
ljobs = jobs.copy()
fjobs = []
today = datetime.today().strftime('%Y%m%d')
#filebasename = f"{indicator_from_file}_{aoiid}_{fdate.replace('-','')}T{ftime.replace(':','')}"
if debug is True:
    while ljobs:
        for i,job in enumerate(ljobs):
            filebasename = job["filebasename"]
            jobId = job["jobId"]
            job_url = status_url.replace("SHIP_JOB_ID", jobId)
            job_status = get_job_status(job_url)
            if job_status != "accepted" and job_status != "running":
                for extension in {"_masked.png", ".xml"}:
                    downfilename = f"{filebasename}{extension}"
                    download_results(downfilename, outdatafolder)
                job["status"] = job_status
                fjobs.append (job)
                ljobs.remove (job)
            #print (f'{root[0].text} - {root[1].text}')
        time.sleep (5) 
        #break 
        clear_output(wait=True)
    download_results(outputfile, outcsvfolder)
else:   # Debug false
    while ljobs:
        for i,job in enumerate(ljobs):
            filebasename = job["filebasename"]
            jobId = job["jobId"]
            job_url = status_url.replace("SHIP_JOB_ID", jobId)
            job_status = get_job_status(job_url)
            if job_status != "accepted" and job_status != "running":
                for extension in {"_masked_bbox.png", ".geojson"}:
                    downfilename = f"{filebasename}{extension}"
                    download_results (downfilename, outdatafolder)
                job["status"] = job_status
                fjobs.append(job)
                ljobs.remove(job)
            # print (f'{root[0].text} - {root[1].text}')
        time.sleep(5)
        #break 
        clear_output(wait=True)
    download_results(outputfile, outcsvfolder)
print("Final results:")
for j in fjobs:
    print(f"{j['filebasename']} - {j['status']} - {j['jobId']}")


# Removing duplicates from csv
import pandas as pd

df = pd.read_csv(os.path.join(outcsvfolder, outputfile))
duplicate_bool = df.duplicated(keep='first')
duplicate = df.loc[duplicate_bool == True]
df.drop_duplicates(keep = 'first', inplace = True)
df.to_csv(os.path.join(outcsvfolder, outputfile), index=False)

""" 
-----------
Merging EG1
-----------
"""

# ##########################
# Merging EG1 entries in csv
# ##########################

df = pd.read_csv(os.path.join(outcsvfolder, outputfile))
eg_entries = df.loc[df['Country'] == 'EG']
eg_time = eg_entries['Time']

date_list=[]
idx_list=[]
for i, v in eg_time.items():
    date_list.append(v)
    idx_list.append(i)

date2merge = []
for i, date_time in enumerate(date_list):
    datehour = date_time[:-9]  # datehour substring without time
    res = [j for j in date_list if datehour in j]
    date2merge.append(res)
    date_list = list(set(date_list) - set(res))
    if not date_list:
        break
date2merge = [x for x in date2merge if x]   # removing empty lists

#from datetime import datetime

sum_det_eg1 = []
merged_data = []
merged_date = []

for i in range(len(date2merge)):
    if len(date2merge[i]) == 1:
        date1 = df.loc[df['Time'] == str(date2merge[i][0])]
        id1 = date1.index.values.astype(int)[0]
        df = df.drop([id1])
    elif len(date2merge[i]) == 2:
        date1 = df.loc[df['Time'] == str(date2merge[i][0])]
        date2 = df.loc[df['Time'] == str(date2merge[i][1])]
        meas1 = int(date1['Measurement Value'])
        meas2 = int(date2['Measurement Value'])
        id1 = date1.index.values.astype(int)[0]
        id2 = date2.index.values.astype(int)[0]
        # Check for 0 in Measurement value
        if meas1 == 0 or meas2 == 0:
            df = df.drop([id1, id2])
        else:
            meas1 = date1['Measurement Value']
            meas2 = date2['Measurement Value']
            sum_meas = int(meas1) + int(meas2)
            sum_det_eg1.append(sum_meas)

            time1 = pd.to_datetime(date1['Time'])
            time2 = pd.to_datetime(date2['Time'])
            merged_date.append([str(time1.values), str(time2.values)])

            hr1 = int(time1.dt.hour)
            min1 = int(time1.dt.minute)
            sec1 = int(time1.dt.second)
            time1_str = str(hr1) + ':' + str(min1) + ':' + str(sec1)
            time1_obj = datetime.strptime(time1_str, '%H:%M:%S').time()
            hr2 = int(time2.dt.hour)
            min2 = int(time2.dt.minute)
            sec2 = int(time2.dt.second)
            time2_str = str(hr2) + ':' + str(min2) + ':' + str(sec2)
            time2_obj = datetime.strptime(time2_str, '%H:%M:%S').time()
            if time1_obj < time2_obj:
                merged_time = date1['Time'].values
            else:
                merged_time = date2['Time'].values
            merged_time = str(merged_time)
            merged_time = merged_time.strip('[]')
            merged_time = merged_time.strip('\'')
            date1.at[id1, 'Time'] = merged_time
            date1.at[id1, 'Measurement Value'] = sum_meas  # working but inserting double index
            df = df.drop([id1, id2])
            df = df.append(date1)
    elif len(date2merge[i]) == 3:
        date1 = df.loc[df['Time'] == str(date2merge[i][0])]
        date2 = df.loc[df['Time'] == str(date2merge[i][1])]
        date3 = df.loc[df['Time'] == str(date2merge[i][2])]

        meas1 = int(date1['Measurement Value'])
        meas2 = int(date2['Measurement Value'])
        meas3 = int(date3['Measurement Value'])

        id1 = date1.index.values.astype(int)[0]
        id2 = date2.index.values.astype(int)[0]
        id3 = date3.index.values.astype(int)[0]

        df = df.drop([id1, id2, id3])
        
merged_csv = outputfile.split('.')[0] + '_merged.csv'
csv_dir = 'csv_merged'
merged_csv_dir = os.path.join(outdatafolder, csv_dir)
if not os.path.exists(merged_csv_dir):
    os.makedirs(merged_csv_dir)
merged_csv_path = os.path.join(merged_csv_dir, merged_csv)
df.to_csv(merged_csv_path, index=False)

# ----------------------------------------
# Thresholding EG1 to new reference value
# ----------------------------------------
eg1_ref_value = 62 # average value over 2019
low_th = eg1_ref_value - .3 * eg1_ref_value
high_th = eg1_ref_value + .3 * eg1_ref_value

df = pd.read_csv(merged_csv_path)
idx = df.index
condition = df['AOI_ID'] == 'EG1'
eg1_idx = idx[condition]
eg1_idx_lst = eg1_idx.tolist()

for i in range(len(eg1_idx_lst)):
    meas_value = df['Measurement Value'].iloc[eg1_idx_lst[i]]
    if meas_value < low_th:
        df['Color code'].iloc[eg1_idx_lst[i]] ='RED'
        df['Reference value'].iloc[eg1_idx_lst[i]] = eg1_ref_value
        df['Indicator Value'].iloc[eg1_idx_lst[i]] = 'Low'
    if meas_value > high_th:
        df['Color code'].iloc[eg1_idx_lst[i]] = 'GREEN'
        df['Reference value'].iloc[eg1_idx_lst[i]] = eg1_ref_value
        df['Indicator Value'].iloc[eg1_idx_lst[i]] = 'High'
    if meas_value >= low_th and meas_value <= high_th:
        df['Color code'].iloc[eg1_idx_lst[i]] = 'BLUE'
        df['Reference value'].iloc[eg1_idx_lst[i]] = eg1_ref_value
        df['Indicator Value'].iloc[eg1_idx_lst[i]] = 'Normal'

df.to_csv(merged_csv_path, index=False)

#--------------------------------------------------------
# Generating csv files for E13c_tri trilateral indicator
# -------------------------------------------------------
new_indicator = 'E13c_trilateral'
df_e13c = pd.read_csv(merged_csv_path)
file_name_tri = outputfile.replace(indicator, new_indicator)
file_name_tri = file_name_tri.split('.')[0] + '_merged.csv'
df_e13c["Description"].replace({"Cargo and Vessels traffic in industrial areas": "Ports and Shipping - Major Harbours"}, inplace=True)
df_e13c["Description"].replace({"Ports and Shipping - Major Harbours (Sentinel-2)": "Ports and Shipping - Major Harbours"}, inplace=True)
df_e13c["Indicator code"].replace({"E200": "E13c"}, inplace=True)
merged_e13c_path = os.path.join(merged_csv_dir, file_name_tri)
df_e13c.to_csv(merged_e13c_path, index=False)


# -----------
# INGESTION
#------------
import subprocess
from ingestion import ingest_to_geoDB

# E200 and E13c_tri ingestion into geoDB
if ingestion_flag:
 
    geodb_logfile = './logs/ships/geodb/geodb_log.csv'
	
    ingest_to_geoDB(merged_csv_path, date_from, date_to, database, geodb_logfile) # ingesting E200 data
    # ingest_to_geoDB(merged_e13c_path, date_from, date_to, database) # ingesting E13c_tri data

# ----------------------------------------- END E200-E13c_tri INGESTION --------------------------------------------

import re
for i in range(len(merged_date)):
    for j in range(len(merged_date[i])):
        str1=merged_date[i][j].split('.')[0]
        merged_date[i][j] = re.sub('[-:\[\']','',str1)

# #######################
# Merging geojson for EG1
# #######################

from os import listdir
from os.path import isfile, join
import shutil

onlyfiles = [f for f in listdir(outdatafolder) if isfile(join(outdatafolder, f))] 
geojson_eg1 = []
geojson_lst = []
for i, file_ in enumerate(onlyfiles):
    if file_.endswith('.geojson') and "EG1" in file_:
        geojson_eg1.append(file_)
    elif file_.endswith('.geojson') and "EG1" not in file_:
        geojson_lst.append(file_)

file2merge = []

import itertools
for i, file_ in enumerate(geojson_eg1):
    str0 = file_.split('_')[2]
    datehour = str0.split('.')[0]
    for item in itertools.chain.from_iterable(merged_date):
        if datehour == item:
            tuple_lst = [(j, date_.index(item))
                        for j, date_ in enumerate(merged_date)
                        if item in date_]
            idx0 = tuple_lst[0][0]
            idx1 = tuple_lst[0][1]
            merged_date[idx0][idx1] = file_ # substituting date with the corresponding json file name


file2merge = merged_date

eg1_json_dir = 'geojson_merged'
eg1_json_path = os.path.join(outdatafolder, eg1_json_dir)   # path to geojson files
isdir = os.path.isdir(eg1_json_path)
if not isdir:
    os.makedirs(eg1_json_path)

for i in range(len(file2merge)):
    if len(file2merge[i])==2:
        file_1 = file2merge[i][0]
        file_2 = file2merge[i][1]
        
        time_1_str = file_1.split('_')[2]
        time_1_str = time_1_str.split('.')[0]
        time1_obj = datetime.strptime(time_1_str, '%Y%m%dT%H%M%S')
        time_2_str = file_2.split('_')[2]
        time_2_str = time_2_str.split('.')[0]
        time2_obj = datetime.strptime(time_2_str, '%Y%m%dT%H%M%S')
            
        if time1_obj > time2_obj:
            time_merged_str = time_2_str
        else:
            time_merged_str = time_1_str
        jsonfile2 = open(os.path.join(outdatafolder, file_2)) # opening second file
        jsonfile2_str = jsonfile2.read()
        start2 = jsonfile2_str.find('{"type": "FeatureCollection", "features": [') + len('{"type": "FeatureCollection", "features": [')
        end2 = jsonfile2_str.find('}}]}') # json file end
        substring2 = jsonfile2_str[start2:end2]+'}}'
        jsonfile1 = open(os.path.join(outdatafolder, file_1))  # opening first file
        jsonfile1_str = jsonfile1.read()
        start1 = jsonfile1_str.find('}}]}') + len(
            '}}]}')-2
        substring1 = jsonfile1_str[:start1] + ', '
        merged_strings = substring1 + substring2 + ']}'
        merged_file_name = file2merge[i][0].split('_')[0] + '_' + file2merge[i][0].split('_')[1] + '_' + time_merged_str + '.geojson'

        merged_file = open(os.path.join(eg1_json_path, merged_file_name), "w")
        n = merged_file.write(merged_strings)

for i in range(len(geojson_lst)):
    shutil.copy(os.path.join(outdatafolder, geojson_lst[i]), os.path.join(eg1_json_path, geojson_lst[i]))


###
# Writing geojson merged path to txt

txt_file = './Outputs/E200/geojson_test.txt'
path_to_write = 'Outputs/Outputs' + eg1_json_path.split('Outputs')[1]
print('path to write to geojson_test.txt: ', path_to_write)
with open(txt_file, 'w') as f:
    f.write(path_to_write)


### End writing to geojson

print('* Ships detection task completed *')
