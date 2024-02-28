import logging
import azure.functions as func
import pyodbc
import requests
import re
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from io import BytesIO
import json
import time
import os
import tempfile
from datetime import datetime, timedelta, timezone
import pymssql
import time
import calendar 
from base64 import b64encode
import math

app = func.FunctionApp()
server_name = "bimageforge.database.windows.net"
database_name = "bimageforge"
username = "forge"
password = "BimageNow2020"

conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server_name};"
        f"Database={database_name};"
        f"UID={username};"
        f"PWD={password};"
    )



@app.schedule(schedule="*/3 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
async def rfi_timer_trigger(myTimer: func.TimerRequest) -> None:
    access_token = await get_access_token()
    logging.info(access_token)
    existence = False
    schema = False
    table = False
    pdf_flag = False
    folder_id = "urn:adsk.wipprod:fs.folder:co.moT-g6mKQzav1hM4JpiThw"
    schema = await create_schema_if_not_exists("timer_function")
    if schema:
        table = await create_table_if_not_exists('timer_function', 'rfi_status')
    
    # Get the current timestamp in UTC
    current_timestamp = datetime.now(timezone.utc)
    current_timestamp_5 = current_timestamp - timedelta(minutes=5)
    logging.info(current_timestamp)
    # Format the timestamp in ISO 8601 format


    # Convert to the desired format
    new_timestamp_str = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
    current_timestamp_minus_5 = current_timestamp_5.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
    print(new_timestamp_str)
    res = select_function_time()
    function_last_runtime = res
    function_last_runtime = function_last_runtime[0]
    # insert_to_function_time()
    # To stop the program


    # original_timestamp = datetime.fromisoformat(current_timestamp_iso.replace('Z', '+00:00'))

    # Convert to the desired format
    # new_timestamp_str = original_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        
         
    # hubs = await get_hubs(access_token)
    # for x in hubs:
    #     if x['attributes']['name'] == "BIMAGE Consulting":

            # id = x['id']
    hub_id ="b.9a1a9f2f-235e-4dc9-b961-29f202ea15ca"
    modified_hub_id = hub_id.replace("b.", "")
    projects = await get_projects(hub_id, access_token)
    for y in projects:

        project_id = y['id']
        # project_id ="b.95674e53-ed24-4a66-b9f0-10e8e0f0befa"
        # folder_id = False
        if project_id =="b.95674e53-ed24-4a66-b9f0-10e8e0f0befa":
            modified_string = project_id.replace("b.", "")
            rfis = await get_rfis(access_token, modified_string)
            for rfi in rfis:
                function_runtime = None
                logging.info(rfi['title'])
                if table:
                    await insert_data('timer_function', 'rfi_status', rfi['id'], rfi['status'], rfi['customIdentifier'], rfi['title'], current_timestamp_minus_5)
                result = await check_rfi_status_in_table('timer_function', 'rfi_status', rfi['id'])
                if result:
                    status, pdf_created, function_runtime = result
                    # print(f"Status: {status}")
                    # print(f"PDF Created: {pdf_created}")
                    logging.info(f'current_timestamp_iso : {new_timestamp_str}')
                    if not function_runtime:
                        function_runtime = current_timestamp_minus_5
                        await update_rfi_table('timer_function', 'rfi_status', rfi['id'],  rfi['status'], "false", current_timestamp_minus_5)

                    if rfi['updatedAt'] > function_last_runtime and new_timestamp_str > rfi['updatedAt']:


                        if rfi['status'] != "open":
                            await update_rfi_table('timer_function', 'rfi_status', rfi['id'],  rfi['status'], "false", new_timestamp_str)

                        if rfi['status'] == "open" :
                            pdf_flag =True
                            # and pdf_created == "false"
                            await update_rfi_table('timer_function', 'rfi_status', rfi['id'],  rfi['status'], "true", new_timestamp_str)
                            logging.info(f"pdf_flag : {pdf_flag}")
                            if pdf_flag:
                                new_report = await generate_rfi_report(access_token, modified_string, rfi['createdAt'])
                                
                                new_report_id = new_report['id']
                                new_report_status = new_report['status']
                                new_report_url = False
                                count = 0 
                                while new_report_status != "complete" and count < 10:
                                    count += 1 
                                    logging.info("-" * 30)
                                    new_report = await get_new_report_url(access_token, project_id, new_report_id)
                                    new_report_status = new_report['status']
                                    try:
                                        new_report_url = new_report['url']
                                    except Exception as ex:
                                        time.sleep(5)
                                        logging.info(ex)
                                if new_report_url:
                                    temp_dir = tempfile.gettempdir()
                                    logging.info(new_report_url)
                                    # Define the local file path in the temporary directory

                                    # Generate a timestamp with the current date and time
                                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

                                    file_name = f"rfi_report_{timestamp}.pdf"
                                    local_file_path = os.path.join(temp_dir, file_name)
                                    logging.info(local_file_path)
                                    download_file_data = await download_file(new_report_url, local_file_path)
                                    if download_file_data:
                                        try:
                                            access_token = await get_access_token()
                                            create_storage_bucket_data = await create_storage_bucket(access_token, project_id, folder_id, file_name)
                                            object_id= create_storage_bucket_data['data']['id']
                                            # Split the string by "/"
                                            parts = object_id.split("/")

                                            # Get the data after the last "/"
                                            if len(parts) > 1:
                                                object_key = parts[-1]
                                                logging.info(f"Data after last slash: {object_key}")
                                            else:
                                                logging.info("No slash found in the string.")
                                            signed_s3_url_data = await get_signed_s3_url(access_token, object_key)
                                            urls = signed_s3_url_data['urls']
                                            uploadKey = signed_s3_url_data['uploadKey']
                                            upload_response_data = await upload_file_to_s3_url(urls, local_file_path)
                                            if upload_response_data:
                                                complete_upload_data = await complete_upload(access_token, object_key, uploadKey)
                                                object_id = complete_upload_data['objectId']
                                                await create_first_version(access_token, project_id, file_name, folder_id, object_id)

                                        except Exception as ex:
                                            logging.info(f"Error in upload - {ex}")
                    else:
                        print("updated before previous trigger.")
                else:
                    logging.info("result error")
                
            # top_folders = await get_top_folders(access_token,hub_id, project_id)
            # top_folders = []
            # for top_folder in top_folders:
            #     folders = await get_folders(access_token, project_id, top_folder['id'])
            #     for folder in folders:
            #         if folder['id'] == folder_id1:
            #             # folder_id = folder_id1
            #             logging.info(folder)
            #             logging.info("^" *40)
            

    if myTimer.past_due:
        logging.info('The timer is past due!')
    update_function_time(new_timestamp_str)

    logging.info('Python timer trigger function executed.')

def select_function_time():
    try:
        schema_name ="timer_function"
        table_name = "rfi_function_last_runtime"
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()
        select_query = f"""
                SELECT function_last_runtime
                FROM {schema_name}.{table_name}
                WHERE id = ?
            """
        cursor.execute(select_query, (1))
        function_last_runtime = cursor.fetchone()

        # Check if data was found
        if function_last_runtime:
            function_last_runtime = function_last_runtime
            
        else:
            pass
            # print("Data not found.")
        # Commit the changes
        conn.commit()
        cursor.close()
        conn.close()
        return function_last_runtime
    except Exception as ex:
        logging.info(ex)

def update_function_time(new_timestamp):
    try:
        schema_name ="timer_function"
        table_name = "rfi_function_last_runtime"
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()
        # Check if a row with the same attributes already exists
        row_count = 0
        if row_count == 0:
            # Insert the new row
            query = f"""
                UPDATE {schema_name}.{table_name} SET function_last_runtime = ?
                WHERE id =1
            """

            # Assuming lastupdatedtime is the current datetime
            current_datetime = datetime.now()

            # Execute the insert query with parameters
            
            # if rfi_status != "open":
            cursor.execute(query, (new_timestamp))
            conn.commit()

            # Close the cursor and connection
            cursor.close()
            conn.close()
            return True
        else:
            logging.info("Update not needed or data not found.")
            return False
    except Exception as ex:
        logging.info(ex)

def insert_to_function_time():
    try: 
        schema_name ="timer_function"
        table_name = "rfi_function_last_runtime"
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()
        # Check if a row with the same attributes already exists
        row_count = 0
        if row_count == 0:
            # Insert the new row
            query = f"""
                INSERT INTO {schema_name}.{table_name} (function_last_runtime)
                VALUES (?)
            """

            # Assuming lastupdatedtime is the current datetime
            current_datetime = datetime.now()

            # Execute the insert query with parameters
            
            # if rfi_status != "open":
            cursor.execute(query, ("2024-02-27T06:35:32.959Z"))
            conn.commit()

            # Close the cursor and connection
            cursor.close()
            conn.close()
            return True
        else:
            logging.info("Update not needed or data not found.")
            return False
    except Exception as ex:
        logging.info(ex)
async def get_time1_and_time2(timestamp_str):

    # timestamp_str = "2024-02-26T00:00:00.920Z"
    original_timestamp = datetime.fromisoformat(timestamp_str[:-1])  # Removing 'Z' and parsing as datetime

    # Subtracting one second
    new_timestamp = original_timestamp - timedelta(seconds=1)

    # Formatting the result back to ISO 8601 format
    new_timestamp_str1 = new_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'


    # print(new_timestamp_str)

    original_timestamp = datetime.fromisoformat(timestamp_str[:-1])  # Removing 'Z' and parsing as datetime

    # Adding one second
    new_timestamp = original_timestamp + timedelta(seconds=1)

    # Formatting the result with milliseconds
    new_timestamp_str2 = new_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

    result = {
        'time1': new_timestamp_str1,
        'time2': new_timestamp_str2
    }
    return result

async def create_schema_if_not_exists(schema_name):
    try:
        # Establish a connection
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()
        # Check if the schema exists
        cursor.execute(f"SELECT schema_id FROM sys.schemas WHERE name = '{schema_name}'")
        schema_exists = cursor.fetchone()

        if not schema_exists:
            # Create the schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA {schema_name}")
            logging.info("Schema created")
            conn.commit()
            return True
        else:
            logging.info("Schema Exists")
            return True
    except Exception as ex:
        logging.info(ex)
        return False
async def create_table_if_not_exists(schema_name, table_name):
    try:
        # Establish a connection
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()
        # Check if the table exists
        # Assuming schema_name and table_name are variables holding the schema and table names
        query = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        """

        cursor.execute(query)
        table_exists = cursor.fetchone()


        if not table_exists:
            # Create the table if it doesn't exist
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{schema_name}.{table_name}')
                    BEGIN
                        CREATE TABLE {schema_name}.{table_name} (
                            id INT IDENTITY(1,1) PRIMARY KEY,
                            rfiid VARCHAR(255) NOT NULL,
                            customIdentifier VARCHAR(255) NOT NULL,
                            title VARCHAR(255) NOT NULL,
                            status VARCHAR(255) NOT NULL,
                            pdf_created VARCHAR(10) NOT NULL,
                            function_runtime VARCHAR(255) NULL,
                            CONSTRAINT rfiid UNIQUE (rfiid)
                        );
                    END;
                    """)
            logging.info("table created")
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            cursor.close()
            conn.close()
            logging.info("Table already exists")
            return True
    except Exception as ex:
        logging.info(ex)
        return False
async def insert_data(schema_name, table_name, rfi_id, rfi_status, rfi_customIdentifier, rfi_title, function_runtime):
    # Establish a connection
    conn = pyodbc.connect(conn_str)

    # Create a cursor object
    cursor = conn.cursor()
    # Check if a row with the same attributes already exists
    query_check = f"""
        SELECT COUNT(*)
        FROM {schema_name}.{table_name}
        WHERE rfiid = ? 
    """
    cursor.execute(query_check, (rfi_id))
    row_count = cursor.fetchone()[0]
    if row_count == 0:
        # Insert the new row
        query = f"""
            INSERT INTO {schema_name}.{table_name} (rfiid, customIdentifier, title, status, pdf_created, function_runtime)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        # Assuming lastupdatedtime is the current datetime
        current_datetime = datetime.now()

        # Execute the insert query with parameters
        
        # if rfi_status != "open":
        cursor.execute(query, (rfi_id, rfi_customIdentifier, rfi_title, rfi_status, "false", function_runtime))
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
        return True
    else:
        logging.info("Update not needed or data not found.")
        return False
async def check_rfi_status_in_table(schema_name, table_name, rfi_id):
    conn = pyodbc.connect(conn_str)

        # Create a cursor object
    cursor = conn.cursor()
    select_query = f"""
            SELECT status, pdf_created, function_runtime
            FROM {schema_name}.{table_name}
            WHERE rfiid = ?
        """
    cursor.execute(select_query, (rfi_id))
    current_status = cursor.fetchone()

     # Check if data was found
    if current_status:
        status, pdf_created, function_runtime = current_status
    else:
        pass
        # print("Data not found.")
     # Commit the changes
    conn.commit()
    cursor.close()
    conn.close()
    return current_status
    
async def update_rfi_table(schema_name, table_name, rfi_id, rfi_status, pdf_created, new_timestamp_str):
    conn = pyodbc.connect(conn_str)
    # Create a cursor object
    cursor = conn.cursor()
    update_query = f"""
        UPDATE {schema_name}.{table_name}
        SET status = ?, pdf_created = ?, function_runtime = ?
        WHERE rfiid = ?
    """

    # Execute the update query with the provided data
    cursor.execute(update_query, (rfi_status, pdf_created, new_timestamp_str, rfi_id))

    # Commit the changes
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Update successful.")
    return True
async def create_first_version(access_token, project_id, file_name, folder_id, object_id):
    try:
        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/items"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

        data = {
            "jsonapi": {"version": "1.0"},
            "data": {
                "type": "items",
                "attributes": {
                    "displayName": file_name,
                    "extension": {"type": "items:autodesk.bim360:File", "version": "1.0"},
                },
                "relationships": {
                    "tip": {"data": {"type": "versions", "id": "1"}},
                    "parent": {
                        "data": {
                            "type": "folders",
                            "id": folder_id,
                        }
                    },
                },
            },
            "included": [
                {
                    "type": "versions",
                    "id": "1",
                    "attributes": {
                        "name": file_name,
                        "extension": {
                            "type": "versions:autodesk.bim360:File",
                            "version": "1.0",
                        },
                    },
                    "relationships": {
                        "storage": {
                            "data": {
                                "type": "objects",
                                "id": object_id,
                            }
                        }
                    },
                }
            ],
        }

        response = requests.post(url, json=data, headers=headers)

        # Check the response
        if response.status_code == 201:
            logging.info("Item created successfully!")
            logging.info(response.json())  # If you want to print the response content
        else:
            logging.info(f"Error in create_first_version : {response.status_code}\n{response.text}")
    except Exception as ex:
        logging.info(f"Error in create_first_version: {ex}")
async def complete_upload(access_token, object_key, uploadKey):
    try:

        url = f"https://developer.api.autodesk.com/oss/v2/buckets/wip.dm.prod/objects/{object_key}/signeds3upload"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        data = {
            "uploadKey": uploadKey,
        }

        response = requests.post(url, json=data, headers=headers)

        # Check the response
        if response.status_code == 200:
            logging.info("Request successful!")
            logging.info(response.json())  # If you want to print the response content
            return response.json()
        else:
            logging.info(f"Error in complete_upload: {response.status_code}\n{response.text}")
    except Exception as ex:
        logging.info(f"Error in complete_upload:{ex}")
        return None
async def upload_file_to_s3_url(urls, local_file_path):
    try:
        url = urls[0]

        file_path = local_file_path

        with open(file_path, "rb") as file:
            file_data = file.read()

        headers = {
            "Content-Type": "application/octet-stream",
        }

        response = requests.put(url, data=file_data, headers=headers)

        # Check the response
        if response.status_code == 200:
            logging.info("File uploaded successfully!")
            return True
        else:
            logging.info(f"Error in upload_file_to_s3_url: {response.status_code}\n{response.text}")
            return False
    except Exception as ex:
        logging.info(f"Error in upload_file_to_s3_url : {ex}")
        return False
async def get_signed_s3_url( access_token, object_id):
    url = f"https://developer.api.autodesk.com/oss/v2/buckets/wip.dm.prod/objects/{object_id}/signeds3upload"
    logging.info(url)
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Print or process the response content
        # logging.info(response.json())
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.info(f"Error in get_signed_s3_url making request: {e}")
async def create_storage_bucket(access_token, project_id, folder_id, file_name):
    logging.info(f"folder id : {folder_id}")
    url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/storage"
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
        "Authorization": f"Bearer {access_token}",
    }

    data = {
        "jsonapi": { "version": "1.0" },
        "data": {
            "type": "objects",
            "attributes": {
            "name": file_name
            },
            "relationships": {
            "target": {
                "data": { "type": "folders", "id": folder_id }
            }
            }
        }
    }   
    logging.info(data)
    response = requests.post(url, json=data, headers=headers)

    # Check the response
    if response.status_code == 201:  # Assuming a successful creation returns a 201 status code
        logging.info(f"Object created successfully! \n {response.text}")
        return response.json()
    else:
        logging.info(f"Error in create_storage_bucket : {response.status_code}\n{response.json()}")
async def get_top_folders(access_token,hub_id, project_id):
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{project_id}/topFolders"
     # logging.info(url)
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Print or process the response content
        # logging.info(response.json())
        return response.json()['data']
    except requests.exceptions.RequestException as e:
        logging.info(f"Error making request: {e}")
async def get_folders(access_token, project_id, top_folder_id):
    url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{top_folder_id}/contents"
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Print or process the response content
        # logging.info(response.json())
        return response.json()['data']
    except requests.exceptions.RequestException as e:
        logging.info(f"Error making request: {e}")
# Function to download the file
async def download_file(url, destination):
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(destination, 'wb') as file:
            file.write(response.content)
        logging.info(f"Downloaded: {destination}")
        return True
    else:
        logging.info(f"Failed to download file. Status code: {response.status_code}")
        return False
async def generate_rfi_report(access_token, project_id, createdAt):
    new_time = await get_time1_and_time2(createdAt)
    logging.info(new_time)
    time1 = new_time['time1']
    time2 = new_time['time2']
    url= f"https://developer.api.autodesk.com/reports/v2/projects/{project_id}/reports"
    headers = {
    "Content-Type": "application/vnd.api+json",
    "Accept": "application/vnd.api+json",
    "Authorization": f"Bearer {access_token}",
    }

    data = {
        "template":"detail","service":"rfis","title":"RFI detail","format":"pdf",
        "options":{"filter":[{"field":"status","operator":"in","value":"open"},
        {"field":"createdAt","operator":"BETWEEN","value":f"{time1}...{time2}"}],
        "sort":["customIdentifier ASC"],"group":[],"components":{"coverPage":{"enabled":True},
        "tableOfContents":{"enabled":True},"photos":{"enabled":True},"references":{"enabled":True},
        "activityLog":{"enabled":True},"comments":{"enabled":True}}}}   
    logging.info(data) 
    response = requests.post(url, json=data, headers=headers)

    # Check the response
    if response.status_code == 201:  # Assuming a successful creation returns a 201 status code
        logging.info(f"File created successfully! \n {response.text}")
        resp = json.loads(response.text)
        resp['created'] = True
        return resp
    else:
        logging.info(f"Error: {response.status_code}\n{response.text}")
async def get_new_report_url(access_token, project_id, new_report_id):
    url = f"https://developer.api.autodesk.com/reports/v2/projects/{project_id}/reports/{new_report_id}"
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        logging.info(response.text)
        return json.loads(response.text)
    except requests.exceptions.RequestException as e:
        logging.info(f"Error making request: {e}")
async def get_access_token():
    # conn = pyodbc.connect(conn_str)

    # cursor = conn.cursor() 

    # get_token_query = "SELECT TOP (1) [email]      ,[access_token]      ,[headers]      ,[expiry_epoch]      ,[expiry]  FROM [dbo].[AccLoginTokens]"
    # cursor.execute(get_token_query)
    # token_data = cursor.fetchone()
    # access_token = token_data.access_token if token_data else None
    # cursor.close()
    # conn.close()
    # access_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjY0RE9XMnJoOE9tbjNpdk1NU0xlNGQ2VHEwUV9SUzI1NiIsInBpLmF0bSI6ImFzc2MifQ.eyJzY29wZSI6WyJhY2NvdW50OnJlYWQiLCJhY2NvdW50OndyaXRlIiwiYnVja2V0OmNyZWF0ZSIsImJ1Y2tldDpyZWFkIiwiYnVja2V0OnVwZGF0ZSIsImJ1Y2tldDpkZWxldGUiLCJkYXRhOnJlYWQiLCJkYXRhOndyaXRlIiwiZGF0YTpjcmVhdGUiLCJkYXRhOnNlYXJjaCIsInVzZXI6cmVhZCIsInVzZXI6d3JpdGUiLCJ1c2VyLXByb2ZpbGU6cmVhZCIsInZpZXdhYmxlczpyZWFkIl0sImNsaWVudF9pZCI6IktrSmZwTVoyZ2NBWEEzZ25EUkdod3Z5UDdaSG1tV25aIiwiaXNzIjoiaHR0cHM6Ly9kZXZlbG9wZXIuYXBpLmF1dG9kZXNrLmNvbSIsImF1ZCI6Imh0dHBzOi8vYXV0b2Rlc2suY29tIiwianRpIjoiQWk4UFRPSmx6WExPamNoTFk5eDc0WGQzQzhNdVBhaXRoV0lQYzdPNGtHNWNkeWhpZlBLVjlFV3NSRkdKTlNueiIsImV4cCI6MTcwODc3OTQ2OCwidXNlcmlkIjoiRjRSMjdaTEhKM0RNRkRENiJ9.G4lfRWmQZpOjUgTsniWjCt2usHBj0ofxkj5-ET5kRhKKlUX-Rh8MBk156-YBzMBhDiUgSkaXjL5z2T2A9ahKJVssspPP7ZTSgW3qYcn8epruogluuTy9O-2RMdC-PTGM-aLgdyw5Q84_svFCgav8S0btl7aJlfubuc0Se9pgjWoVYxGu_oGUwHkIqw1uty9gyhLfM6zzqsBmEfGligs7Byu7nLMzSk7s2hiOYpKOX5V4oTUxEDIok6hfzSBqnl5a_CT4e_1msODOE2zmgr3mPr_Bzj0abHxwOwxaaxKn3wV7cJmNBx3fRDIuksa5xWQaGJLS6LOAuX68wSSEJEUt7g"


    conn = pymssql.connect('bimageforge.database.windows.net', 'forge', 'BimageNow2020', 'bimageforge')
    cursor = conn.cursor()
        
    cursor.execute('select access_token from AccLoginTokens where expiry > getdate()')

    access_token = ''

    for r in cursor:    
        access_token = r[0]
        print(access_token)
    
    if access_token == '':
        cursor.execute('select headers from AccLoginTokens')
        for r in cursor:    
            headers = json.loads(r[0])  
                
            rs = requests.get('https://login.acc.autodesk.com/api/v1/authentication/refresh?currentUrl=https%3A%2F%2Facc.autodesk.com%2Fprojects', headers=headers, data='')
            
            js = json.loads(rs.text)
            access_token = js['accessToken']
            expiry = js['expiresAt']
            cursor.execute(f"update AccLoginTokens set access_token = '{access_token}' , expiry = '{expiry}'")
            conn.commit()

    return access_token
async def get_rfis(access_token, project_id):
    limit = 2
    result = []
    j= 0
    url = f'https://developer.api.autodesk.com/bim360/rfis/v2/containers/{project_id}/rfis?limit={limit}&offset={j}'
    # logging.info(url)
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        totalResults = response.json()['pagination']['totalResults']
        rounded_range  = math.ceil(totalResults/limit)
        logging.info(rounded_range )
        for i in range(rounded_range+1 ):
            url = f'https://developer.api.autodesk.com/bim360/rfis/v2/containers/{project_id}/rfis?limit={limit}&offset={j}'

        # logging.info(response.json())
            response = requests.get(url, headers=headers)
            j +=2
            for x in response.json()['results']:
                result.append(x)
        return result
    except requests.exceptions.RequestException as e:
        logging.info(f"Error making request: {e}")
async def get_projects(hub_id,access_token):
    url = f'https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects'
    # logging.info(url)
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Print or process the response content
        return response.json()['data']
    except requests.exceptions.RequestException as e:
        logging.info(f"Error making request: {e}")
async def get_hubs(access_token):
    url = 'https://developer.api.autodesk.com/project/v1/hubs'
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Print or process the response content
        # logging.info(response.json())
        return response.json()['data']
    except requests.exceptions.RequestException as e:
        logging.info(f"Error making request: {e}")