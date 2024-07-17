import requests
import pandas
import pyodbc
import os
from dotenv import load_dotenv, find_dotenv

def dbconn():
    try:
        server = os.getenv("Epic_VS_ServerName")
        db = os.getenv("Epic_VS_DB_Name")
        uid = os.getenv("Epic_VS_DB_UserName")
        pwd = os.getenv("Epic_VS_Password")
        driver = 'ODBC Driver 17 for SQL Server'
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+uid+';PWD='+pwd+';')
    except Exception as err:
        print('Database could not be connected due to Error: %s' % err.text)
    return cnxn

def insert_valuestreamowners(df):
    try:
        cnxn = dbconn()
        cursor = cnxn.cursor()        
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO [land].[value_stream_owners] (person_key,pillar_key,value_stream_key,sub_value_stream_key,pillar_guuid,value_stream_guuid,substream_guuid,pillar_owner_email,value_stream_owner_email,pillar_name,value_stream_name,sub_value_stream_name,job_title,job_level_indicator,job_family,job_level_description) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.person_key,row.pillar_key,row.value_stream_key,row.sub_value_stream_key,row.pillar_guuid,row.value_stream_guuid,row.substream_guuid,row.pillar_owner_email,row.value_stream_owner_email,row.pillar_name,row.value_stream_name,row.sub_value_stream_name,row.job_title,row.job_level_indicator,row.job_family,row.job_level_description)
        cnxn.commit()
        cursor.close()
        print('Data Inserted')
    except Exception as err:
        print('Exception: ' + str(err))

def main():
    try:
        dotenv_path=find_dotenv()
        load_dotenv(dotenv_path)
        url_base = os.getenv("url_base_valuestreamowners")
        api_key = os.getenv("api_key_valuestreamowners")
        #this can be json or csv
        format_identifier = "json"
        url = f"{url_base}?api_key={api_key}&format={format_identifier}".format(url_base, api_key, format_identifier)
        #print(url)
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        responseData = response.json()
        #normalize json data
        df = pandas.json_normalize(responseData, max_level=1, record_path=['pillar_value_stream_owners'] )  
        insert_valuestreamowners(df)
    except Exception as err:
        print('Exception: ' + str(err))
        
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()