import os
import sys
from xcube_geodb.core.geodb import GeoDBClient
import shutil
import pandas as pd
from geopandas import GeoDataFrame
import geopandas as gpd
from shapely.geometry import Point, mapping
from datetime import datetime
import csv

"""
The xcube-geodb consists of a Restful service and a Python client
"""

#ingestion_log_dir = f"./logs/ships/geodb/"  # for manual execution
#log_file = "./logs/ships/geodb/geodb_log.csv"

def prepare_column_names (pd):
    pd.columns = [column.split("[")[0].split("(")[0] for column in pd.columns]
    pd.columns = [column.lstrip().rstrip().lower() for column in pd.columns]
    pd.columns = [column.replace(" ", "_").replace("-", "_") for column in pd.columns]
    return pd


def write_log(today,proctime,db,indicator,file_path,start_date,end_date,status,log_file):
    print('Writing to log')
    #ingestion_log_file = open(os.path.join(ingestion_log_dir,'ingestion_log.csv'), 'a', newline='')
    print('file path:', file_path)
    #log_file = "./ingestor/ingestion_log.csv"
    ingestion_log_file = open(log_file, 'a', newline='')
    print(ingestion_log_file)
    wr = csv.writer(ingestion_log_file)
    date_time_file = today + proctime
    wr.writerow([date_time_file, db, indicator, file_path, start_date, end_date, status])
    ingestion_log_file.close()


def ingest_to_geoDB(input_file, date_start, date_end, db, log_file):
    date_from = date_start
    date_to = date_end
    database = db
    file_name_path = input_file

    file_name = input_file.split('/')[-1]
    # input('wait in ingestion utils')
    indicator_code1 = file_name.split('_')[0]
    indicator_code2 = file_name.split('_')[1]


    test_table = None
    if indicator_code2 == 'trilateral':
        indicator_code2 = '_tri'
        test_table = indicator_code1+indicator_code2
        # print('test table 2', test_table)
    else:
        test_table = indicator_code1
        # print('test table 1: ', test_table)

    current_time = datetime.now().time()
    proc_time = current_time.strftime("T%H%M%S")
    print('Connecting to geoDB...')

    # GEODB access credentials (from May 2021)
    geodb = GeoDBClient(server_url="https://xcube-geodb.brockmann-consult.de",
                        server_port=443,
                        client_id="3671d89fbddb4c0f963b965aedf2dc49",
                        client_secret="PIl0dy_6mN3KWFL4B3gwjnb3o493eSwc4XTC7I-c8tw",
                        auth_mode="client-credentials",
                        #auth_mode = 'silent',
                        auth_aud="https://xcube-gen.brockmann-consult.de/api/v2/"
                       )

    print(geodb.whoami)

    print(geodb.get_properties(test_table, database=database))

    print('collection content:\n', geodb.get_my_collections(database=database))
    print('getting current collections on database ', database)
    collection_df = geodb.get_my_collections(database=database)
    print(type(collection_df))
    print(collection_df.head())
    print(collection_df.columns)
    # print(collection_df[['table_name']]==test_table)
    tab_df = collection_df[['collection']]==test_table # old db column name 'table_name' (changed on 1 July 2022)
    tab_lst = tab_df.values.tolist()
    flat_lst = [item for sublist in tab_lst for item in sublist]
    print('No. table with name {}: {} '.format(test_table, sum(flat_lst)))  # counting no. table with a specific name

    today = datetime.today().strftime('%Y%m%d')

    # ------------------------------------------------------
    # Check if data collection is present on the selected db
    # -------------------------------------------------------
    if sum(flat_lst) == 0:
        print('No data collection for {} is present in {}'.format(test_table, database))
        ingestion_status = 'Failed. Data collection not present in db.'
        write_log(today,proc_time,database,test_table,file_name_path,date_from,date_to,ingestion_status)
        sys.exit("Ingestion terminated.")


    # -----------------------------------------------
    # Reading input file and checking if it is empty
    # -----------------------------------------------
    pd_data = pd.read_csv(file_name_path, delimiter=",").fillna("/")

    if pd_data.empty:
        ingestion_status='Failed. Input file empty'
        write_log(today,proc_time,database,test_table,file_name_path,date_from,date_to,ingestion_status)
        sys.exit("Input file empty. Ingestion terminated.")

    # -----------------------------------
    # Saving current collection to csv
    # -----------------------------------
    print('Saving current collection to csv...')

    ingestion_bkp_dir = f"./geodb_bkp/{test_table}/{database}/{today}{proc_time}/"

    if not os.path.exists(ingestion_bkp_dir):
        os.makedirs(ingestion_bkp_dir)

    test_data_collection = geodb.get_collection(test_table, database=database)
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    collection_bkp = test_table + '_' + database + '_bkp_' + now + '.csv'
    # print('collection bkp: ', collection_bkp)
    test_data_collection.to_csv(os.path.join(ingestion_bkp_dir, collection_bkp), index=False)
    print('Collection saved to csv.')

    # ----------------------------------------------------------
    # Deleting collection data for N1 or N1_tri before ingestion
    # ----------------------------------------------------------
    if test_table == 'N1' or test_table == 'N1_tri':
        # input('Deleting data from collection?')
        print('Deleting data from N1 before ingestion...')
        geodb.delete_from_collection(test_table, query="id=gt.0", database=database)
        print(geodb.get_collection(test_table, database=database))
        print('Data deleted.')

    # ---------------------------------------------------------------------
    # Preparing pandas DataFrame
    # - replacing UK with GB
    # - replacing old indicator E13c with E13d
    # - adding _S2 to E1, E1a, E2 (indicators dismissed, replaced by E200)
    # ---------------------------------------------------------------------
    if pd_data['Data Provider'][0]=='NTNU_USF_UTFPR'and pd_data['Indicator code'][0] == 'E13c':
        pd_data['Reference time']="2018-05-01"
        pd_data["Country"].replace({"UK": "GB"}, inplace=True)
        pd_data["Indicator code"].replace({"E13c":"E13d"},inplace=True)

    if pd_data['Indicator code'][0] == 'E1' or pd_data['Indicator code'][0] == 'E1a' or pd_data['Indicator code'][0] == 'E2':
        # print(pd_data["Indicator code"][0])
        pd_data["Indicator code"].replace({pd_data['Indicator code'][0]:pd_data['Indicator code'][0]+"_S2"},inplace=True)


    table_name = test_table
    print('Data collection to be updated: ', table_name)
    # print('########################################')
    # print('looking at the csv data to ingest')
    # print(pd_data.head(30))

    # Prepare column names
    prepare_column_names(pd_data)

    # Prepare geometry and create geopandas DataFrame
    points = pd_data.apply(lambda row: Point(reversed([float(coord) for coord in row.aoi.split(",")])), axis=1) # We switch the coordiante order, since geopandas always use (x,y)
    gpd_data = gpd.GeoDataFrame(pd_data, geometry = points)
    gpd_data.crs = {"init": "epsg:4326"}
    # print(gpd_data.head())
    # print('\nData to ingest:\n', gpd_data)
    # print('###########################################')
    # print('Check data in given collection ' + table_name)
    # Check data in a given collection
    gdf = geodb.get_collection(table_name, database=database)
    # print(gdf)

    try:
        ## Import data into data collection (table)
        ingestion_status = geodb.insert_into_collection(table_name, gpd_data, crs=4326, database=database)
        print('Ingestion status: ', ingestion_status)
    except:
        ingestion_status = 'Error while importing'
        print('Ingestion status: ', ingestion_status)

    # -------------------
    # Writing to log file
    # -------------------
    write_log(today,proc_time,database,test_table,file_name_path,date_from,date_to,ingestion_status,log_file)
    # ----------------------------------
