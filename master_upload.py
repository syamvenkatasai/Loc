import argparse
import ast
import base64
import json
import requests
import traceback
import warnings
import os
import pysftp
import shutil
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import create_engine
import psutil
from pathlib import Path
from multiprocessing import Pool
from functools import partial

from datetime import datetime, timedelta
from db_utils import DB
from flask import Flask, request, jsonify
from flask_cors import CORS
from pandas import Series, Timedelta, to_timedelta
from time import time
from itertools import chain, repeat, islice, combinations
from io import BytesIO,StringIO
from app.elasticsearch_utils import elasticsearch_search
from py_zipkin.util import generate_random_64bit_string
from py_zipkin import storage
from collections import defaultdict
from sqlalchemy.orm import sessionmaker
from elasticsearch import Elasticsearch
import re


from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
import pytz
tmzone = 'Asia/Kolkata'

from ace_logger import Logging

from app import app
from app import cache
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span


import io

es_dns = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_DNS','')
es_port = os.environ.get('ELASTIC_SEARCH_FULL_PORT', '')
es_scheme = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_SCHEME','')


es = Elasticsearch(
    [f'{es_dns}'],
    http_auth=('elastic','MagicWord'),
    scheme=f"https",
    port=es_port,
    ca_certs="/usr/share/elastic-auth/elasticsearch-ca.pem",
)

logging = Logging(name='master_upload')

db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD']
}



def http_transport(encoded_span):
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    body = encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes


def insert_into_audit(data):
    tenant_id = "hdfc"
    db_config['tenant_id'] = tenant_id
    groupdb = DB('group_access', **db_config)
    groupdb.insert_dict(data, 'hdfc_audit')
    return True

def generate_multiple_insert_query(data, table_name):
    values_list = []
    for row in data:
        values_list_element = []
        for column, value in row.items():
            values_list_element.append(f"'{value}'")
        values_list.append('(' + ', '.join(values_list_element) + ')')
    values_list = ', '.join(values_list)
    columns_list = ', '.join([f"`{x}`" for x in list(data[0].keys())])
    query = f"INSERT INTO `{table_name}` ({columns_list}) VALUES {values_list}"
    
    return query

def create_index(tenant_ids, sources=[]):
    body = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "date_detection": "false",
            "numeric_detection": "false"
        }
    }
    
    body_with_date = {
            "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                          "lowercase"
                        ]
                    }
                }
            }
            },
        "mappings": {
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "@version": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ocr": {
                    "properties": {
                        "created_date": {
                            "type": "date"
                        },
                        "last_updated": {
                            "type": "date"
                        }
                    }
                },
                "process_queue": {
                    "properties": {
                        "created_date": {
                            "type": "date"
                        },
                        "last_updated": {
                            "type": "date"
                        },
                        "freeze": {
                            "type": "boolean"
                        },
                        "case_lock":{
                            "type": "boolean"
                        }
                    }
                }
            },
            "date_detection": "false",
            "numeric_detection": "false"
        }
        }
    indexes = []
    
    for tenant_id in tenant_ids:
        indexes.extend(get_search_indexes(sources, tenant_id))

    logging.info(f"####indexes are {indexes}")
    for ind in indexes:
        es.indices.delete(index=ind, ignore=[400, 404])
        if 'processqueue' in ind:
            es.indices.create(index=ind, body=body_with_date, ignore=400)
        else:
            logging.info(f"###Creating index in progress...")
            es.indices.create(index=ind, body=body, ignore=400)
            logging.info(f"###Index created")

def get_search_indexes(sources, temp_tenant_id=''):
    """
    Author : Akshat Goyal
    :param sources:
    :return:
    """
    tenant_id = temp_tenant_id.replace('.', '')
    if not sources:
        return '_all'
    indexes = []
    if isinstance(sources, list):
        for source in sources:
            new_source = tenant_id + source if tenant_id else source
            new_source = new_source.replace('.', '').replace('_', '')
            indexes.append(new_source)
    elif isinstance(sources, str):
        new_source = tenant_id + '_' + sources if tenant_id else sources
        new_source = new_source.replace('.', '').replace('_', '')
        indexes.append(new_source)

    return indexes

def dataframe_to_blob(data_frame):
    
    chunk_size = 10000

    bio = BytesIO()


    writer = pd.ExcelWriter(bio, engine='xlsxwriter')

    # Write the DataFrame to the Excel file in chunks
    for i in range(0, len(data_frame), chunk_size):
        data_frame_chunk = data_frame.iloc[i:i+chunk_size]
        data_frame_chunk.to_excel(writer, index=False, startrow=i)

    # Close the ExcelWriter to flush data to the file
    writer.save()

    
    bio.seek(0)

    blob_data = base64.b64encode(bio.read())

    # Return the Base64 encoded blob data
    return blob_data

def fix_json_decode_error(data_frame):
    for column in data_frame.columns:
        if isinstance(data_frame.loc[0, column], (pd._libs.tslibs.timedeltas.Timedelta, pd._libs.tslibs.timestamps.Timestamp)):       
            data_frame[column] = data_frame[column].astype(str)
    return data_frame

def get_user_groups(tenant_id):
    db_config['tenant_id'] = tenant_id
    group_db = DB('group_access', **db_config)
    queue_db = DB('queues', **db_config)

    query = "SELECT id, username from active_directory"
    user_list = group_db.execute(query).username.to_dict()

    query = "SELECT * from user_organisation_mapping where type = 'user'"
    user_details = group_db.execute(query).to_dict(orient='records')

    query = "SELECT * from organisation_attributes"
    attributes_df=group_db.execute_(query)
    attributes = group_db.execute(query, index = 'att_id').to_dict()

    query = "SELECT * from organisation_hierarchy"
    hierarchy = group_db.execute(query).set_index('h_group').to_dict()['h_order']
    
    query = "SELECT id,group_definition from group_definition"
    group_definition = group_db.execute(query).group_definition.to_dict()
    
    user_sequence = {}
    for user_detail in user_details:
        try:
            user_sequence[user_detail['sequence_id']].append(user_detail)
        except:
            user_sequence[user_detail['sequence_id']] = [user_detail]
            
    attribute_dropdown = group_db.get_all('attribute_dropdown_definition')   
    
    attribute_dropdown['attribute_id'] = attribute_dropdown['attribute_id'].apply(lambda x: attributes_df['attribute'])
               
    user_info = defaultdict(dict)
    for k, v in user_sequence.items():
        for user_detail in v:
            name = user_list[user_detail['user_id']]
            index = user_detail['organisation_attribute']
            attribute_name = attributes['attribute'][index]
            attribute_value = user_detail['value']             
            try:
                if attribute_name in user_info[k][name]:
                    user_info[k][name][attribute_name] = ','.join(set(user_info[k][name][attribute_name].split(',') + [attribute_value]))
                else:
                    user_info[k][name][attribute_name] = attribute_value
            except:
                user_info[k][name] = {attribute_name: attribute_value}
                               
            for key, val in hierarchy.items():
                if attribute_name in val.split(','):
                    attribute_loop = val.split(attribute_name+',')[1].split(',') if len(val.split(attribute_name+',')) > 1 else val
                    for child_attribute in attribute_loop:
                        condition = (attribute_dropdown['parent_attribute_value'] == attribute_value) & (attribute_dropdown['attribute_id'] == child_attribute)
                        query_result = attribute_dropdown[condition]
                        if not query_result.empty:
                            child_attribute_value = list(query_result.value.unique())
                            user_info[k][name][child_attribute] =  ','.join(child_attribute_value)
                    
    # Optimize below   
    group_dict = defaultdict(dict)
    for key_, val_ in user_info.items():
        for k,v in val_.items():
            group_list = []
            for key, val in v.items():
                subset = []
                val = val.split(',')
                for i in val:
                    for group, attribute in group_definition.items(): 
                        attribute = json.loads(attribute)
                        for x,y in attribute.items():
                            if key == x and i == y:
                               subset.append(group)
                if subset!= []:
                    group_list.append(subset)
            group_dict[key_][k] = group_list
            
    classify_users = defaultdict(dict)
    for key, val in group_dict.items():           
        for user, value in val.items():
            if value and len(value) > 1:
                classify_users[key][user] = list(set.intersection(*map(set,value)))
            else:
                if len(value) > 0:
                    classify_users[key][user] = value[0]
                else:
                    pass

    return classify_users

def get_group_ids(user, db):
    print(f'Getting group IDs for user `{user}`')

    query = 'SELECT organisation_attributes.attribute, user_organisation_mapping.value \
            FROM `user_organisation_mapping`, `active_directory`, `organisation_attributes` \
            WHERE active_directory.username=%s AND \
            active_directory.id=user_organisation_mapping.user_id AND \
            organisation_attributes.id=user_organisation_mapping.organisation_attribute'

    user_group = db.execute_(query, params=[user])

    if user_group.empty:
        logging.error(f'No user organisation mapping for user `{user}`')
        return

    user_group_dict = dict(zip(user_group.attribute, user_group.value))
    user_group_dict = {key: [value] for key, value in user_group_dict.items()}
    group_def_df = db.get_all('group_definition')

    if group_def_df.empty:
        logging.debug(f'Groups not defined in `group_definition`')
        return

    group_def = group_def_df.to_dict(orient='index')
    group_ids = []
    for index, group in group_def.items():
        logging.debug(f'Index: {index}')
        logging.debug(f'Group: {group}')

        try:
            group_dict = json.loads(group['group_definition'])
        except:
            logging.error('Could not load group definition dict.')
            break

        # Check if all key-value from group definition is there in the user group
        if group_dict.items() == user_group_dict.items():
            group_ids.append(index)

    print(f'Group IDs: {group_ids}')
    return group_ids



def structure_to_excel(table_name, db, tenant, database):
    try:
        query = 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name ="'+table_name+'" AND TABLE_SCHEMA LIKE "'+tenant+'_'+database+'"'
        df =db.execute_(query)
        print(f"THIS IS THE SELECT STATEMENT {query}")
        print(f"THIS IS THE OUTPUT {df}")
        df1= df.rename({'TABLE_SCHEMA':'database', 'TABLE_NAME':'table_name', 'COLUMN_NAME':'columns','COLUMN_TYPE':'COLUMN_TYPE', 'CHARACTER_MAXIMUM_LENGTH':'length','IS_NULLABLE':'index'}, axis=1)
        print(df1.columns)
        df1=df1[['database', 'table_name', 'columns','COLUMN_TYPE','index','COLUMN_KEY','EXTRA']]
        df1['datatype'] = df1['COLUMN_TYPE'].str.split('(').str[0]
        df1['length'] = df1['COLUMN_TYPE'].str.split('(').str[1]
        df1['length'] = df1['length'].str.split(')').str[0]
        df1=df1[['database', 'table_name', 'columns','datatype','length','index','COLUMN_KEY','EXTRA']]
        df2=df1.replace({'index': r'^N.$'}, {'index': 'not null'}, regex=True)
        df3=df2.replace({'index': r'^YE.$'}, {'index': 'null'}, regex=True)
        df4= df3.replace({'length' : { 'None' : 1}})
        df5=df4.replace(to_replace ="UNI", value ="unique key") 
        df6=df5.replace(to_replace ="PRI", value ="primary key")
        df7=df6[['database', 'table_name', 'columns','datatype','length','index','COLUMN_KEY','EXTRA']]

        blob_data = dataframe_to_blob(df7)
        
        message = 'Successfuly converted structure to excel.'
        return {"flag": True, "message" : message, "blob_data": blob_data}
    except:
        traceback.print_exc()
        message= f"Something went wrong while converting the structure to excel"
        return {"flag": True, "message": message}



@app.route('/folder_monitor_sftp', methods=['POST', 'GET'])
def folder_monitor_sftp():
    data = request.json
    print(f'Request data: {data}')
    tenant_id = data.get('tenant_id', None)
    try:
        input_path_str = "/var/www/master_upload/app/master_input/"
        output_path = "/var/www/master_upload/app/master_output/"
        file_list = os.listdir(input_path_str)
        if len(file_list):
            files = [file_list[0]]
        else:
            files = []
        reponse_data = {}
        file_names = []
        for file_ in files:
            logging.debug(f'move from: {file_}')
            reponse_data['move_from'] = str(file_)
            filename = file_
            file_name = input_path_str+'/'+f'{file_}'
            try:
                shutil.move(Path(file_name),Path(output_path))
            except Exception as e:
                out_path = output_path+"/"+f'{file_}'
                destination_path = Path(out_path)
                os.remove(destination_path)
                shutil.move(Path(file_name),Path(output_path))
            file_names.append({'file_name': filename})
        logging.debug(f'Files: {file_names}')
        reponse_data['files'] = file_names
        reponse_data['workflow'] = 0

        final_response_data = {"flag": True, "data": reponse_data}
        return jsonify(final_response_data)
    except:
        logging.exception(
                'Something went wrong watching folder. Check trace.')
        final_response_data = {'flag': False, 'message': 'System error! Please contact your system administrator.'}
        return jsonify(final_response_data)
        


@app.route('/get_files_from_sftp_masters', methods = ['GET', 'POST'])
def get_files_from_sftp_masters():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and time calc")
        pass

    trace_id = generate_random_64bit_string()
    tenant_id = os.environ.get('TENANT_ID',None)

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='get_files_from_sftp_masters',
            span_name='get_files_from_sftp_masters',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

        hostname = 'dpcftpuat.hbctxdom.com'
        username = 'metaecbf'
        password = 'Hdfc@12345'
        port = 22
        source_file_path = '/CREOUT'
        destination_file_path = '/var/www/master_upload/app/master_input'

        print(f"###Connecting to SFTP server: {hostname} with username: {username}")

        try:
            source_pdf_files = []
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None

            with pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts) as sftp:
                print("Connected to SFTP server")
                sftp.cwd(source_file_path)
                files = sftp.listdir()
                source_pdf_files = [file for file in files if file.endswith('.csv') or file.endswith('.xlsx')]
                print("List of PDF files found in source path: ", source_pdf_files)

                for filename in source_pdf_files:
                    source_file_path_ = f"{source_file_path}/{filename}"
                    destination_file_path_ = f"{destination_file_path}/{filename}"
                    print(f"Copying file from {source_file_path_} to {destination_file_path_}")
                    sftp.get(source_file_path_, destination_file_path_)
                    sftp.remove(source_file_path_)
                    print(f"Successfully received and deleted file: {filename}")

            print("Successfully copied files from SFTP server")

        except Exception as e:
            print("## Exception: Something went wrong in getting SFTP files", e)



@app.route('/upload_master_blob_sftp', methods = ['GET', 'POST'])
def upload_master_blob_sftp():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    print(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    file_name = data.pop('file', '')
    file_name = file_name['file_name']
    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id)

    with zipkin_span(
                service_name='master_upload',
                zipkin_attrs=attr,
                span_name='upload_master_blob',
                transport_handler=http_transport,
                sample_rate=0.5):
        try:
            table_name = file_name.split(".")[0].lower()
            #Converting table name to the suitable table name formate
            if 'partymaster' in table_name or 'Partymaster' in table_name or 'Party_Master' in table_name or 'party_master' in table_name:
                table_name = 'party_master'
            elif 'citystate' in table_name or 'Citystate' in table_name or 'RM_Master' in table_name or 'rm_master' in table_name:
                table_name = 'city_state'
            elif 'age_margin_master' in table_name or 'Age_Margin_Master' in table_name or 'agemarginmaster' in table_name or 'AgeMarginMaster' in table_name:
                table_name = 'age_margin_working_uat'
            elif 'component_master' in table_name or 'Component_Master' in table_name:
                table_name = 'component_master'
            elif 'wbo_region_master' in table_name or 'WBO_Region_Master' in table_name or 'wboregionmaster' in table_name or 'WBORegionMaster' in table_name:
                table_name = 'wbo_region'
    
            table_name=table_name.lower()
            logging.info(f'#####table name is ---> {table_name}')
            file_name = f'/var/www/master_upload/app/master_output/{file_name}'
            ext=Path(file_name).suffix
            excel_data=''
            if ext=='.csv':
                # Extended list of possible delimiters
                possible_delimiters = ['~', ';', '|', '$', ',', '\t', ':', ' ', '^', '&', '%', '#', '@', '!', '*', '?', '/']
                
                for delim in possible_delimiters:
                    try:
                        excel_data = pd.read_csv(file_name, delimiter=delim)
                        logging.info(f"Successfully read CSV file using delimiter: {delim}")
                        break
                    except pd.errors.ParserError as e:
                        logging.warning(f"Delimiter {delim} failed: {e}")
                else:
                    logging.info(f"CSV file could not be read with any known delimiters")
                
            elif ext=='.xlsx':
                excel_data = pd.read_excel(file_name,engine='openpyxl')
            else:
                message = "Not Supported Format"
                data = {'message': message}
                return_data = {'flag': True, 'data': data}
                return jsonify(return_data)
            
            excel_data.columns = excel_data.columns.str.lower()

            if 'id' in excel_data.columns:
                excel_data = excel_data.drop(columns=['id'])
            if 'last_updated' in excel_data.columns:
                excel_data = excel_data.drop(columns=['last_updated'])
            if 'last_updated_by' in excel_data.columns:
                excel_data = excel_data.drop(columns=['last_updated_by'])

            try:
                excel_data.replace(re.compile(r'^(n[ou]*ll|n[oa]*ne|nan)$', re.IGNORECASE), '', inplace=True)
            except:
                logging.info(f"#######NONE, NAN and null values are not removing while uploading")

            columns = excel_data.columns
            columns=columns.to_list()
            column = []
            for i in columns:    
                i = i+" VARCHAR2(255)"
                column.append(i)
            query = 'SELECT table_name FROM user_tables'
            df = extraction_db.execute_(query)
            table_names_list=df['table_name'].to_list()
            table = table_name.upper()
            data = excel_data
            data = data.astype(str)
            print(f'Data is: {data}')
            table_name=table_name.lower()

            if table_name == 'age_margin_working_uat':
                try:
                    data['COMPOSITE_KEY'] = data['PARTY_ID'].astype(str) + data['COMPONENT_NAME'].astype(str) + data['AGE'].astype(str)
                except:
                    data['COMPOSITE_KEY'] = data['party_id'].astype(str) + data['component_name'].astype(str) + data['age'].astype(str)
                data = data.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                data.reset_index(drop=True, inplace=True)
                data = data.drop(columns=['COMPOSITE_KEY'])
                if table_name == 'age_margin_working_uat':
                    try:
                        def remove_decimals(age):
                            if age:
                                return age.split('.')[0]
                            return age
                        data['age'] = data['age'].apply(remove_decimals)
                    except:
                        logging.info(f"##error in the age conversion")
                        pass

            try:
                current_ist = datetime.now(pytz.timezone(tmzone))
                currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                logging.info(f"####currentTS now is {currentTS}")
                data['LAST_UPDATED'] = currentTS
            except:
                pass

            ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
            os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
            str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
            engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
            connection = engine.raw_connection()
            cursor = connection.cursor()
            try:
                trunc_query = f'TRUNCATE TABLE {table_name}'
                extraction_db.execute_(trunc_query)
                logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
            except Exception as e:
                logging.info(f"## Exception occured while truncation data ..{e}")
                pass
            data_tuples = [tuple(row) for row in data.to_numpy()]
            columns = ','.join(data.columns)
            placeholders = ','.join([':' + str(i+1) for i in range(len(data.columns))])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            logging.info(f"### Query is {sql} for {table_name}")
            cursor.executemany(sql, data_tuples)
            connection.commit()
            cursor.close()
            connection.close()
            create_index(tenant_ids= [tenant_id], sources= [table_name])
            print('Created Index')
            message = "Successfully Updated"
            data = {'message': message}
            return_data = {'flag': True, 'data': data}
        except Exception as e:
            traceback.print_exc()
            message = f"Could not update {table_name}. Please check the template/data."
            data = {'message': message}
            return_data = {'flag': True, 'data': data}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(return_data)







@app.route('/upload_master_blob', methods = ['GET', 'POST'])
def upload_master_blob():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    print(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    duplicate_check = data.pop('duplicate_check', False)
    last_updated_by = data.get('user', None)
    insert_flag = data.pop('insert_flag', 'append')

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id)

    with zipkin_span(
                service_name='master_upload',
                zipkin_attrs=attr,
                span_name='upload_master_blob',
                transport_handler=http_transport,
                sample_rate=0.5):

        try:
            master_table_name = data.pop('master_table_name')
        except:
            traceback.print_exc()
            message = "Master table name not provided"
            return jsonify({"flag": False, "message" : message})

        try:
            database, table_name = master_table_name.split('.')
        except:
            traceback.print_exc()
            message = "Master table name is in incorrect format. Check configuration."
            return jsonify({"flag": False, "message": message})

        try:
            blob_data = data.pop('blob')
        except:
            traceback.print_exc()
            message = "Blob data not provided"
            return jsonify({"flag": False, "message" : message})

        db_config['tenant_id'] = tenant_id
        table_db = DB(database, **db_config)
        extraction_db = DB('extraction', **db_config)
        duplicate_blob = None

        logging.debug('trying to convert blob to dataframe')
        try:
            blob_data = blob_data.split(",", 1)
            extension=blob_data[0].split("/",1)[1].split(";")[0]
            file_blob_data=blob_data[1]
            # Padding
            file_blob_data += '='*(-len(file_blob_data)%4)
            file_stream = BytesIO(base64.b64decode(file_blob_data))

            ## Handling both csv and excel files from front end upload
            if extension=='csv':
                data_frame = pd.read_csv(file_stream)  
            else:
                data_frame = pd.read_excel(file_stream,engine='openpyxl')
            data_frame['last_updated_by'] = last_updated_by
            data_frame.fillna(value= '', inplace=True)
            columns_ = list(data_frame.columns.values)
            print(f"columns names are : {columns_}")
            try:
                columns_.remove('id') #ALL COLUMNS EXCEPT ID 
            except Exception:
                pass
            if 'last_updated' in columns_:
                columns_.remove('last_updated') #ALL COLUMNS EXCEPT ID 
                
            if 'last_updated_by' in columns_:
                columns_.remove('last_updated_by')

            #to replace '/' with '-' from date fields it is causing error in search
            try:
                date_column = [col for col in data_frame.columns if 'date' in col.lower()]
                for col in date_column:
                    data_frame[col] = data_frame[col].str.replace('/', '-')
            except:
                pass
        except:
            message = "Could not convert blob to dataframe"
            logging.exception(message)
            return jsonify({"flag": False, "message" : message})

        try:
            master_df = extraction_db.master_execute_(f"SELECT * FROM `{table_name}`")
            column_names = master_df.columns.tolist()
            columns = []
            values_to_drop = ['id','last_updated','last_updated_by']
            column_names = list(filter(lambda x: x not in values_to_drop, column_names))
            print(f"master sheet columns names are{column_names}")
            print("comparing uploaded sheet columns and master sheet columns")
            print(f"master_upload {set (columns_ )}")
            if set (columns_ ) == set (column_names):
                print("checking the comparision of both sheets, columns are same")
            else:
                print("please check both sheets columns names should match")
                return {'flag': False, 'message': "Columns do not match. Download the excel and get the columns"}
        except:
            print("Error while uploading sheet")
            return_data = ({"flag": False, "message" : "error uploading xl sheet"})
        print(f"checking for the values of duplicate check and insert flag {duplicate_check},{insert_flag}")

        #remove NONE, NAN and null values
        try:
            data_frame.replace(re.compile(r'^(n[ou]*ll|n[oa]*ne|nan)$', re.IGNORECASE), '', inplace=True)
        except:
            logging.info(f"#######NONE, NAN and null values are not removing while uploading")

        if duplicate_check == True and insert_flag == 'append':
            try:
                master_df = extraction_db.execute_(f"SELECT * FROM `{table_name}`")
                

                master_df.replace(to_replace= 'None', value= '', inplace= True)
                master_df.fillna(value= '', inplace= True)

                data_frame.replace(to_replace= 'nan', value= '', inplace= True)
                data_frame.fillna(value= '', inplace= True)

                
                columns = columns_


                records_to_upload = len(data_frame)

                data_frame.drop_duplicates(subset= columns_, keep = 'first', inplace = True)

                master_df = master_df.astype(str)
                data_frame = data_frame.astype(str)
                df_all = data_frame.merge(master_df[columns], how = 'left', on= columns, indicator = True)

                unique_df = df_all[df_all['_merge'] == 'left_only'].drop(columns = ['_merge'])
                duplicates_df = df_all[df_all['_merge'] == 'both'].drop(columns = ['_merge']) #NEED THIS FOR KARVY

                records_uploaded = len(unique_df)
                print(f"length of unique records{len(unique_df)}")
                print(f"number of unique records{unique_df}")
                no_of_duplicates = records_to_upload - records_uploaded
                print(f"number of duplicates{no_of_duplicates}")
                if len(unique_df)==0:
                    print('Entering if condition')
                    message = f"There were no unique values to insert"
                    return_data = {"flag": True, "message" : message}
                    return jsonify(return_data)
                if 'last_updated' in unique_df.columns and 'id' in unique_df.columns:
                    df_to_insert = unique_df.drop(columns = ['id','last_updated'])
                elif 'id' in unique_df.columns:
                    df_to_insert = unique_df.drop(columns = ['id'])
                elif 'last_updated' in unique_df.columns:
                    df_to_insert = unique_df.drop(columns = ['last_updated'])
                print(f"Data to be inserted after duplicate check - ", df_to_insert)
                df_to_insert.drop(columns=['last_updated_by'], inplace=True)
                if table_name == 'age_margin_working_uat':
                    logging.info(f"### table is age_margin_working_uat")
                    current_ist = datetime.now(pytz.timezone(tmzone))
                    currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                    logging.info(f"####currentTS now is {currentTS}")
                    df_to_insert['LAST_UPDATED'] = currentTS

                    try:
                        def remove_decimals(age):
                            if age:
                                return age.split('.')[0]
                            return age
                        df_to_insert['age'] = df_to_insert['age'].apply(remove_decimals)
                    except:
                        logging.info(f"##error in the age conversion")
                        pass

                ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                'AlgoTeam_123'  + '@' + os.environ['HOST_IP'] + ':' + \
                str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                data_tuples = [tuple(row) for row in df_to_insert.to_numpy()]
                columns = ','.join(df_to_insert.columns)
                placeholders = ','.join([':' + str(i+1) for i in range(len(df_to_insert.columns))])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                logging.info(f"###  Query is {sql} \n INSERTION  SUCCESSFUL FOR {table_name}")
                cursor.executemany(sql, data_tuples)
                connection.commit()
                cursor.close()
                connection.close()
                
                duplicates_df = duplicates_df.astype(str)
                duplicates_df.replace(to_replace= 'None', value= '', inplace= True)
                duplicate_blob = dataframe_to_blob(duplicates_df)
                print(f"{duplicates_df} is converted into a binary blob")


                ##### creating elastic search index of data
                create_index(tenant_ids= [tenant_id], sources= [table_name])
                print('Created Index')

                #updating last updated by
                query = f"update `master_upload_tables` set last_updated_by = 'NULL' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                print(f"inserted by :{inserting}")
                time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                last_updated = extraction_db.execute_(time_query).to_dict()
                print(f"updated time by :{last_updated}")

                message = f"Successfully updated {table_name} in {database}. {no_of_duplicates} duplicates ignored out of total {records_to_upload} records."
                return_data = {'flag': True, 'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message, 'blob' : duplicate_blob.decode('utf-8')}

            except Exception:
                traceback.print_exc()
                message = f"Could not append data to {table_name}"
                return_data = {"flag": False, "message" : message}

        elif duplicate_check == False and insert_flag == 'append':
            try:
                if 'last_updated' in data_frame.columns and 'id' in data_frame.columns:
                    data_frame = data_frame.drop(columns = ['id','last_updated'])
                elif 'id' in data_frame.columns:
                    data_frame = data_frame.drop(columns = ['id'])
                elif 'last_updated' in data_frame.columns:
                    data_frame = data_frame.drop(columns = ['last_updated'])
                print(f'data frame is {data_frame}')
                data_frame.drop(columns=['last_updated_by'], inplace=True)
                if table_name == 'age_margin_working_uat':
                    logging.info(f"### table is age_margin_working_uat")
                    current_ist = datetime.now(pytz.timezone(tmzone))
                    currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                    logging.info(f"####currentTS now is {currentTS}")
                    data_frame['LAST_UPDATED'] = currentTS

                    try:
                        def remove_decimals(age):
                            if age:
                                return age.split('.')[0]
                            return age
                        data_frame['age'] = data_frame['age'].apply(remove_decimals)
                    except:
                        logging.info(f"##error in the age conversion")
                        pass
                ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                'AlgoTeam_123'  + '@' + os.environ['HOST_IP'] + ':' + \
                str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                data_tuples = [tuple(row) for row in data_frame.to_numpy()]
                columns = ','.join(data_frame.columns)
                placeholders = ','.join([':' + str(i+1) for i in range(len(data_frame.columns))])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                print(sql)
                cursor.executemany(sql, data_tuples)
                connection.commit()
                cursor.close()
                connection.close()
                
                create_index(tenant_ids= [tenant_id], sources= [master_table_name])
                print('Created Index')

                #updating last updated by
                query = f"update `master_upload_tables` set last_updated_by = 'NULL' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                print(f"inserted by :{inserting}")                
                time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                last_updated = extraction_db.execute_(time_query).to_dict()
                print(f"updated time by :{last_updated}")

                message = f"Successfully updated in {database}."
                return_data = {'flag': True, 'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message}

            except:
                traceback.print_exc()
                message = f"Could not update {table_name}"
                return_data = {"flag": False, "message" : message}

        elif insert_flag == 'overwrite':    
            try:
                if table_name == 'age_margin_working_uat':

                    # query = f'SELECT * from age_margin_working_uat'
                    # res = extraction_db.execute_(query)
                    # res = res.fillna('')
                    # res.replace('null', '', inplace=True)
                    # res.replace('NULL', '', inplace=True)
                    # res.replace('Null', '', inplace=True)
                    # res.replace('None', '', inplace=True)
                    # res.replace('NONE', '', inplace=True)
                    # res.replace('none', '', inplace=True)
                    # res.replace('nan', '', inplace=True)

                    # res = res.drop(columns=[ 'id','last_updated', 'PARTY_ID', 'BANKING_TYPE','MARGIN_TYPE', 'DP_MARGIN_CONSIDERATION', 'STOCK_STATEMENT_TYPE','BANKING_SHARE', 'COMPONENT_NAME', 'AGE', 'MARGIN', 'ID','LAST_UPDATED'])
                    # try:
                    #     res['age'] = res['age'].astype(float)
                    #     res['age'] = res['age'].astype(int)
                    #     res['age'] = res['age'].astype(str)
                    # except:
                    #     pass


                    if 'id' in data_frame.columns:
                        data_frame = data_frame.drop(columns = ['id'])
                    data_frame = data_frame.astype(str)
                    if 'last_updated_by' in data_frame.columns:
                        data_frame.drop(columns=['last_updated_by'], inplace=True)
                    data_frame = data_frame.fillna('')
                    data_frame.replace('null', '', inplace=True)
                    data_frame.replace('NULL', '', inplace=True)
                    data_frame.replace('Null', '', inplace=True)
                    data_frame.replace('None', '', inplace=True)
                    data_frame.replace('NONE', '', inplace=True)
                    data_frame.replace('none', '', inplace=True)
                    data_frame.replace('nan', '', inplace=True)

                    try:
                        data_frame['age'] = data_frame['age'].astype(float)
                        data_frame['age'] = data_frame['age'].astype(int)
                        data_frame['age'] = data_frame['age'].astype(str)
                    except:
                        pass

                    # result = pd.concat([res, data_frame], ignore_index=True)
                    # data_frame = result
                    
                    print(f'Result is: {data_frame}')
                    
                    try:
                        data_frame['COMPOSITE_KEY'] = data_frame['PARTY_ID'].astype(str) + data_frame['COMPONENT_NAME'].astype(str) + data_frame['AGE'].astype(str)
                    except:
                        data_frame['COMPOSITE_KEY'] = data_frame['party_id'].astype(str) + data_frame['component_name'].astype(str) + data_frame['age'].astype(str)
                    df_unique = data_frame.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                    df_unique.reset_index(drop=True, inplace=True)
                    df_unique = df_unique.drop(columns=['COMPOSITE_KEY'])

                    if table_name == 'age_margin_working_uat':
                        logging.info(f"### table is age_margin_working_uat")
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        df_unique['LAST_UPDATED'] = currentTS
                        try:
                            def remove_decimals(age):
                                if age:
                                    return age.split('.')[0]
                                return age
                            df_unique['age'] = df_unique['age'].apply(remove_decimals)
                        except:
                            logging.info(f"#####")
                            pass

                    ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                    'AlgoTeam_123'  + '@' + os.environ['HOST_IP'] + ':' + \
                    str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                    engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                    connection = engine.raw_connection()
                    cursor = connection.cursor()
                    try:
                        trunc_query = f'TRUNCATE TABLE {table_name}'
                        extraction_db.execute_(trunc_query)
                        logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
                    except Exception as e:
                        logging.info(f"## Exception occured while truncation data ..{e}")
                        pass
                    
                    logging.info(f"####age values ----> {df_unique['age']}")
                    data_tuples = [tuple(row) for row in df_unique.to_numpy()]
                    columns = ','.join(df_unique.columns)
                    placeholders = ','.join([':' + str(i+1) for i in range(len(df_unique.columns))])
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    print(sql)
                    cursor.executemany(sql, data_tuples)
                    connection.commit()
                    cursor.close()
                    connection.close()

                    create_index(tenant_ids= [tenant_id], sources= [table_name])
                    print('Created Index')

                    #updating last updated by

                    query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                    inserting = extraction_db.execute_(query)
                    print(f"inserted by :{inserting}")                
                    time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                    last_updated = extraction_db.execute_(time_query).to_dict()
                    print(f"updated time by :{last_updated}")

                    message = f"Successfully updated in {database}."
                    return_data = {'flag': True,'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message}


                else:
                    print(f'table name is {table_name} and dataframe is {data_frame}')
                    
                    if 'id' in data_frame.columns:
                        data_frame = data_frame.drop(columns = ['id'])
                    data_frame = data_frame.astype(str)
                    data_frame.drop(columns=['last_updated_by'], inplace=True)
                    if table_name == 'age_margin_working_uat':
                        logging.info(f"### table is age_margin_working_uat")
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        data_frame['LAST_UPDATED'] = currentTS

                        try:
                            def remove_decimals(age):
                                if age:
                                    return age.split('.')[0]
                                return age
                            data_frame['age'] = data_frame['age'].apply(remove_decimals)
                        except:
                            logging.info(f"##error in the age conversion")
                            pass
                    ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                    'AlgoTeam_123'  + '@' + os.environ['HOST_IP'] + ':' + \
                    str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                    engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                    connection = engine.raw_connection()
                    cursor = connection.cursor()
                    try:
                        trunc_query = f'TRUNCATE TABLE {table_name}'
                        extraction_db.execute_(trunc_query)
                        logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
                    except Exception as e:
                        logging.info(f"## Exception occured while truncation data ..{e}")
                        pass
                    data_tuples = [tuple(row) for row in data_frame.to_numpy()]
                    columns = ','.join(data_frame.columns)
                    placeholders = ','.join([':' + str(i+1) for i in range(len(data_frame.columns))])
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    print(sql)
                    cursor.executemany(sql, data_tuples)
                    connection.commit()
                    cursor.close()
                    connection.close()

                    ##### creating elastic search index of data          
                    create_index(tenant_ids= [tenant_id], sources= [table_name])
                    print('Created Index')

                    #updating last updated by

                    query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                    inserting = extraction_db.execute_(query)
                    print(f"inserted by :{inserting}")                
                    time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                    last_updated = extraction_db.execute_(time_query).to_dict()
                    print(f"updated time by :{last_updated}")

                    message = f"Successfully updated in {database}."
                    return_data = {'flag': True,'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message}

            except:
                traceback.print_exc()
                message = f"Could not update {table_name}. Please check the template/data."
                return_data = {"flag": False, "message" : message}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return return_data

        
@app.route('/download_master_blob', methods = ['GET', 'POST'])
def download_master_blob():
    data = request.json
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    print(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    tenant_id = tenant_id.replace('.acelive.ai','')
    download_type = data.pop('dowload_type', 'Data')
    
    file_type=data.get('file_type','xlsx')

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id)

    with zipkin_span(
        service_name='download_master_blob',
        zipkin_attrs=attr,
        span_name='download_master_blob',
        transport_handler=http_transport,
        sample_rate=0.5):

        try:
            master_table_name = data.pop('master_table_name')
        except:
            traceback.print_exc()
            message = f"Master table name not provided"
            return jsonify({"flag": False, "message" : message})

        try:
            database, table_name = master_table_name.split('.')
        except:
            traceback.print_exc()
            message = f"Master table name is in incorrect format. Check configuration."
            return jsonify({"flag": False, "message": message})

        db_config['tenant_id'] = tenant_id
        table_db = DB(database,**db_config)
        
            
        if download_type == 'Data':
            try:
                data_frame = table_db.master_execute_(f"SELECT * FROM `{table_name}`")
                data_frame.fillna(value= '')
                data_frame = data_frame.astype(str)
                try:
                    data_frame.replace(re.compile(r'^(n[ou]*ll|n[oa]*ne|nan)$', re.IGNORECASE), '', inplace=True)
                except:
                    logging.info(f"#######NONE, NAN and null values are not removing while uploading")
                
                try:
                    data_frame = data_frame.drop(columns = ['last_updated'])

                    temp_dir=Path('/app/master_download')
                    if file_type=="csv":
                        temp_file=Path(f'{table_name}.csv')
                        logging.info(f"### Writing df to excel file ....")
                        data_frame.to_csv(temp_dir / temp_file,index=False)
                        return jsonify({'flag': True, 'file_name' : str(temp_file)})
                    else:
                        temp_file=Path(f'{table_name}.xlsx')
                        logging.info(f"### Writing df to excel file ....")
                        data_frame.to_excel(temp_dir / temp_file,index=False)
                        return jsonify({'flag': True, 'file_name' : str(temp_file)})
                except Exception as e:
                    logging.info(f"## Error OcCuured {e} ")
                
            except:
                traceback.print_exc()
                message = f"Could not load from {master_table_name}"
                return jsonify({"flag": False, "message" : message})
        elif download_type == 'template':
            try:
                data_frame = table_db.master_execute_(f"SELECT * FROM `{table_name}` LIMIT 0")
                data_frame = data_frame.astype(str)
                data_frame.replace(to_replace= 'None', value= '', inplace= True)
                blob_data = dataframe_to_blob(data_frame)
            except:
                traceback.print_exc()
                message = f"Could not load from {master_table_name}"
                return jsonify({"flag": False, "message" : message})
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify({'flag': True, 'blob': blob_data.decode('utf-8'), 'file_name' : master_table_name + '.xlsx'})
       
def remove_duplicates(dict_list):
    # A set to keep track of unique dictionaries (excluding 'id' key)
    seen = []
    unique_list = []
    # print(f" #### DICT LIST IS {dict_list}")
    for d in dict_list:
        # Create a new dictionary excluding the 'id' key
        #dict_without_id = tuple((k, v) for k, v in d.items() if k != 'ID')
        dict_without_id = {k: v for k, v in d.items() if k.lower() != 'id' and k.lower()!='last_updated'}
        #dict_tuple = tuple(sorted(dict_without_id.items())
        # If this dictionary (excluding 'id') hasn't been seen, add it to the result
        if dict_without_id not in seen:
            seen.append(dict_without_id)
            unique_list.append(d)
    print(f"### UNQUE LEN IS {len(unique_list)}")

    return unique_list





def master_search(tenant_id, text, table_name, start_point, offset, columns_list, header_name):
    elastic_input = {}
    
    print(f'#######{tenant_id}')
    print(f'#######{table_name}')
    print(f'#######{start_point}')
    print(f'#######{offset}')
    print(f'#######{columns_list}')
    print(f'#######{header_name}')
    elastic_input['columns'] = columns_list
    elastic_input['start_point'] = start_point
    elastic_input['size'] = offset
    if header_name:
        header_name=header_name.upper()
        elastic_input['filter'] = [{'field': header_name, 'value': "*" + text + "*"}]
    else:
        elastic_input['text'] = text
    elastic_input['source'] = table_name
    elastic_input['tenant_id'] = tenant_id
    print(f"output of the elastic_input---------{elastic_input}")
    files, total = elasticsearch_search(elastic_input)
    ## to handle duplicate data if there is any
    logging.info(f"###files are {files}")
    
    print(f"output----------{files,total}")
    return files, total
    
@app.route('/elastic_search_test', methods= ['GET', 'POST'])
def elastic_search_test():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    print(f'Request data: {data}')
    try:
        tenant_id = data['tenant_id']
    except:
        traceback.print_exc()
        message = f"tenant_id not provided"
        return jsonify({"flag": False, "message" : message})
    
    attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='ace_template_training',
            zipkin_attrs=attr,
            span_name='train',
            transport_handler=http_transport,
            sample_rate=0.5
    ):    
        try:
            text = data['data'].pop('search_word')
            table_name = data['data'].pop('table_name')
            start_point = data['data']['start'] - 1
            end_point = data['data']['end']
            header_name = data['data'].get('column', None)
            offset = end_point - start_point
        except:
            traceback.print_exc()
            message = f"Input data is missing "
            return jsonify({"flag": False, "message" : message})
        
        db_config['tenant_id'] = tenant_id
        extraction = DB('extraction', **db_config)
        columns_list = list(extraction.execute_(f"SHOW COLUMNS FROM `{table_name}`")['Field'])
    
        files, total = master_search(tenant_id = tenant_id, text = text, table_name = table_name, start_point = 0, offset = 10, columns_list = columns_list, header_name=header_name)
        
        if end_point > total:
            end_point = total
        if start_point == 1:
            pass
        else:
            start_point += 1
        
        pagination = {"start": start_point, "end": end_point, "total": total}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
            
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        
        return jsonify({"flag": True, "data": files, "pagination":pagination})
        
def button_options(extraction_db):
    try:   
        button_options_query=f"SELECT * FROM `button_options`"
        button_options_query_df=extraction_db.execute_(button_options_query)
        button_options_query_df.to_dict(orient='records')
        for idx, row in button_options_query_df.iterrows():
            if row['type'] == 'dropdown':
                button_options_query_df.at[idx, 'options'] = json.loads(row['options'])
        button_list=[]
        options_data={}
        for row in button_options_query_df['button'].unique():
            button_list=[]
            button_options_df =button_options_query_df[button_options_query_df['button']==row]
            for display_name in button_options_df['display_name'].unique():
                button_option_df=button_options_query_df[button_options_query_df['display_name']==display_name]
                button_df=button_option_df.to_dict(orient='records')
                button_list.append({"display_name":display_name,"options":button_df})
            options_data[row]=button_list
        return {"flag":True,"options_data":options_data }
    except:
        traceback.print_exc()
        message = f"something went wrong while generating button_options data"
        return {"flag": False, "message" : message} 
    

def check_if_record_exists(table_db,table_name,data_dict):
    
    where_clause = []
    where_value_list = []


    for where_column, where_value in data_dict.items():
        if where_value==None or where_value=='NULL' or where_value=='null' or where_column=='None' or type(where_value)==int or where_value==False or where_value==True:
            where_clause.append(f"`{where_column}`=%s")
        elif len(where_value)>4000 or where_column=='last_updated':
            where_clause.append(f"`{where_column}`=%s")
        else:
            where_clause.append(f"TO_CHAR(`{where_column}`)=%s")
        where_value_list.append(where_value)
    where_clause_string = ' AND '.join(where_clause)

    select_query=f"select count(*) from {table_name} where {where_clause_string}"

    result = list(table_db.execute_(select_query,params=where_value_list)["COUNT(*)"])[0]

    if result>0:
        return True
    else:
        return False



def find_changes(modified_data,original_data):
    modified_data_list = modified_data
    old_data_list = original_data

    fields_changed = []
    new_values = []
    old_values = []

    # Iterate through each pair of modified and old data
    for modified_data, old_data in zip(modified_data_list, old_data_list):
        change_details = {
            "id": modified_data.get("id"),
            "fields_changed": [],
            "new_values": {},
            "old_values": {}
        }
        
        # Iterate over the fields in old_data
        for key in old_data:
            old_value = old_data.get(key)
            new_value = modified_data.get(key)

            # Check if the value has changed
            if old_value != new_value:
                change_details["fields_changed"].append(key)
                change_details["new_values"][key] = new_value
                change_details["old_values"][key] = old_value

        if change_details["fields_changed"]:
            fields_changed.append({change_details["id"]: change_details["fields_changed"]})
            new_values.append({change_details["id"]: change_details["new_values"]})
            old_values.append({change_details["id"]: change_details["old_values"]})

    return fields_changed, new_values, old_values

@app.route('/update_master_data', methods = ['GET', 'POST'])
def update_master_data():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    try:
        data = request.json
        print(f'Request data: {data}')
        tenant_id = data.get('tenant_id', None)
        
        attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
            )
        with zipkin_span(
            service_name='master_upload',
            span_name='update_master_data',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
                
           
            master_table_name = data.get('master_table',"")
            username = data.get('user',"")
            modified_data = data.get('modified_data','')
           

            if master_table_name=="":
                message="############ Master table is not defined"
                print(message)
                return jsonify({"flag":False,"message":message})

            try:
                database, table_name = master_table_name.split('.')
            except:
                traceback.print_exc()
                message = "Master table name is in incorrect format. Check configuration."
                print(f"############{message}")
                return jsonify({"flag": False, "message": message})
            
            db_config['tenant_id'] = tenant_id
            table_db = DB(database, **db_config)
            extraction_db = DB('extraction', **db_config)

            for record in modified_data:
                logging.info(f"###in for loop record is {record}")
                record_type = record.pop("__type","")
               
                if record_type=="":
                    continue
                if record_type=="new":
                    logging.info(f"###in for loop new")
                    duplicate_check = check_if_record_exists(table_db,table_name,record)
                    if not duplicate_check:
                        try:
                            del record['last_updated']
                        except:
                            pass
                        try:
                            del record['LAST_UPDATED']
                        except:
                            pass
                        try:
                            del record['id']
                        except:
                            pass
                        try:
                            del record['ID']
                        except:
                            pass
                        #Data base triggers are not correct timestamp for last_updated
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        record['LAST_UPDATED'] = currentTS
                        print(f'Record is: {record}')
                        print(f'Table is : {table_name}')
                        table_db.insert_dict(record,table_name)
                        
                            




                elif record_type=="update":
                    logging.info(f"###in for loop update")
                    duplicate_check = check_if_record_exists(table_db,table_name,record)
                    if not duplicate_check:
                        logging.info(f"##in updating loop")
                        try:
                            record_id=record.pop("id")
                        except:
                            pass
                        try:
                            record_id=record.pop("ID")
                        except:
                            pass
                        try:
                            record.pop("last_updated")
                        except:
                            pass
                        try:
                            record.pop("LAST_UPDATED")
                        except:
                            pass
                        #Data base triggers are not correct timestamp for last_updated
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        record['LAST_UPDATED'] = currentTS
                        logging.info(f"###record is {record} , record_id is {record_id}")
                        table_db.update(table_name,update=record,where={"id":record_id})
                            

                    
                elif record_type=="remove":
                    logging.info(f"###in for loop remove")
                    duplicate_check = check_if_record_exists(table_db,table_name,record)
                    if duplicate_check:
                        try:
                            record_id=record.pop("id")
                        except:
                            pass
                        try:
                            record_id=record.pop("ID")
                        except:
                            pass
                        delete_query = f"DELETE from {table_name} where id={record_id}"

                        
                        table_db.execute_(delete_query)

            if table_name == 'age_margin_working_uat':

                query = f'SELECT * from age_margin_working_uat order by last_updated ASC'
                logging.info(f"####query is {query}")
                res = extraction_db.execute_(query)
                res = res.fillna('')
                res.replace('null', '', inplace=True)
                res.replace('NULL', '', inplace=True)
                res.replace('Null', '', inplace=True)
                res.replace('None', '', inplace=True)
                res.replace('NONE', '', inplace=True)
                res.replace('none', '', inplace=True)
                res.replace('nan', '', inplace=True)

                res = res.drop(columns=[ 'id','last_updated', 'PARTY_ID', 'BANKING_TYPE','MARGIN_TYPE', 'DP_MARGIN_CONSIDERATION', 'STOCK_STATEMENT_TYPE','BANKING_SHARE', 'COMPONENT_NAME', 'AGE', 'MARGIN', 'ID','LAST_UPDATED'])
                try:
                    def remove_decimals(age):
                        if age:
                            return age.split('.')[0]
                        return age
                    res['age'] = res['age'].apply(remove_decimals)
                except:
                    logging.info(f"##error in the age conversion")
                    pass
                data_frame = res
                try:
                    data_frame['COMPOSITE_KEY'] = data_frame['PARTY_ID'].astype(str) + data_frame['COMPONENT_NAME'].astype(str) + data_frame['AGE'].astype(str)
                except:
                    data_frame['COMPOSITE_KEY'] = data_frame['party_id'].astype(str) + data_frame['component_name'].astype(str) + data_frame['age'].astype(str)
                df_unique = data_frame.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                df_unique.reset_index(drop=True, inplace=True)
                df_unique = df_unique.drop(columns=['COMPOSITE_KEY'])

                
                current_ist = datetime.now(pytz.timezone(tmzone))
                currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                logging.info(f"####currentTS now is {currentTS}")
                df_unique['LAST_UPDATED'] = currentTS


                ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                try:
                    trunc_query = f'TRUNCATE TABLE {table_name}'
                    extraction_db.execute_(trunc_query)
                    logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
                except Exception as e:
                    logging.info(f"## Exception occured while truncation data ..{e}")
                    pass
                data_tuples = [tuple(row) for row in df_unique.to_numpy()]
                columns = ','.join(df_unique.columns)
                placeholders = ','.join([':' + str(i+1) for i in range(len(df_unique.columns))])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                print(sql)
                cursor.executemany(sql, data_tuples)
                connection.commit()
                cursor.close()
                connection.close()
                create_index(tenant_ids= [tenant_id], sources= [table_name])
                print('Created Index')

            message="Successfully modified the master data table"
            print(f"##############{message}")

            response_data = {"flag":True,"data":{"message":message}}

    except Exception as e:
        print("############### ERROR In Updating master data")
        logging.exception(e)
        message = "Something went wrong while updating the master data"
        response_data = {"flag": False, "message": message}

    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / \
            (1024 * 1024 * 1024)
        end_time = tt()
        memory_consumed = f"{memory_consumed:.10f}"
        print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
        time_consumed = str(round(end_time-start_time,3))
    except:
        logging.warning("Failed to calc end of ram and time")
        logging.exception("ram calc went wrong")
        memory_consumed = None
        time_consumed = None
        pass
    
    print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
    return jsonify(response_data)

def master_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'master_data_audit')
    return True


@app.route('/get_master_data', methods= ['GET', 'POST'])
def get_master_data():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass    

    data = request.json
    trace_id = generate_random_64bit_string()
    print(f'Request data: {data}')
    try:
        tenant_id = data['tenant_id']
        tenant_id = tenant_id.replace('.acelive.ai','')
    except:
        traceback.print_exc()
        message = f"tenant_id not provided"
        return jsonify({"flag": False, "message" : message})
    
    attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='master_upload',
            zipkin_attrs=attr,
            span_name='get_master_data',
            transport_handler=http_transport,
            sample_rate=0.5
    ):    
        master_table_name = data.pop('master_table_name', None)
        user_name = data.pop('user', None)
        database = 'extraction'
        db_config['tenant_id'] = tenant_id
        extraction_db = DB(database, **db_config)
        flag = data.pop('flag', '')
        if flag == 'search':
            try:
                text = data['data'].pop('search_word')
                master_table_name = data['data'].pop('table_name')
                start_point = data['data']['start'] - 1
                end_point = data['data']['end']
                header_name = data['data'].get('column', None)
                offset = end_point - start_point
            except:
                traceback.print_exc()
                message = f"Input data is missing "
                return jsonify({"flag": False, "message" : message})    
            
            db_config['tenant_id'] = tenant_id
            try:
                database, table_name = master_table_name.split('.')
            except:
                traceback.print_exc()
                message = f"Master table name is in incorrect format. Check configuration."
                return jsonify({"flag": False, "message": message})
            try:
                table_db = DB(database, **db_config)
                columns_list = list(table_db.execute_(f"SHOW COLUMNS FROM `{table_name}`")['field'])
            except:
                logging.exception(f"Could not execute columns query for the table {table_name}")
            print(f'############Entering master_search function')
            files, total = master_search(tenant_id = tenant_id, text = text, table_name = table_name, start_point = start_point, offset = offset, columns_list = columns_list, header_name=header_name)

            print(f'#########Files got are: {files}')
            
            if len(files)>0:
                displayed_columns=list(files[0].keys())
            else:
                displayed_columns=[]
            dropdown_dict = {}
            if table_name=='party_master':
                if len(files)>0:
                    displayed_columns = ['ID','PARTY_ID','PARTY_NAME','RELATION_MGR_EMP_CODE','RELATION_MGR']
                    df = pd.DataFrame(files)
                    #removing TRL contanied rows
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[~df['PARTY_ID'].astype(str).str.contains('TRL')]
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if table_name=='city_state':
                if len(files)>0:
                    displayed_columns = ['RM_MGR_CODE','RM_CITY','RM_STATE']
                    try:
                        files = [{k: ("" if v is None else v) for k, v in record.items()} for record in files if "TRL" not in record["RM_MGR_CODE"]]
                        files = [{"RM_MGR_CODE":record["RM_MGR_CODE"], "RM_CITY":record["RM_CITY"], "RM_STATE":record["RM_STATE"]} for record in data]
                    except Exception as e:
                        print(e)
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if table_name=='component_master':
                if len(files)>0:
                    displayed_columns = ['COMPONENT_CODE','COMPONENT_NAME','STATUS','COMPONENT_CATEGORY','COMPONENT_TYPE']
                    df = pd.DataFrame(files)
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if table_name=='age_margin_working_uat' or table_name=='age_margin_working':
                if len(files)>0:
                    displayed_columns = ['ID','PARTY_ID','BANKING_TYPE','MARGIN_TYPE','DP_MARGIN_CONSIDERATION','STOCK_STATEMENT_TYPE','BANKING_SHARE','COMPONENT_NAME','AGE','MARGIN']
                    df = pd.DataFrame(files)
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    query = f'select DISTINCT(component_name) from age_margin_working_uat'
                    component_name_df = extraction_db.execute_(query)
                    component_name_list = component_name_df['COMPONENT_NAME'].to_list()
                    dropdown_dict = {}
                    dropdown_dict['component_name']=component_name_list
                else:
                    displayed_columns = []

            if table_name=='wbo_region':
                if len(files)>0:
                    displayed_columns=['RM_STATE','WBO_REGION']
                    df = pd.DataFrame(files)
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                else:
                    displayed_columns=[]
            if end_point > total:
                end_point = total
            if start_point == 1:
                pass
            else:
                start_point += 1
            
            try:
                files_=remove_duplicates(files)
                print(f"LNETHTH DSI IS {len(files_)}")
            except Exception as e:
                print(f"exefdajkfndan fsfkd ..{e}")
                pass
            
            pagination = {"start": start_point, "end": len(files_), "total": total}
            
            try:
                memory_after = measure_memory_usage()
                memory_consumed = (memory_after - memory_before) / \
                    (1024 * 1024 * 1024)
                end_time = tt()
                time_consumed = str(end_time-start_time)
            except:
                logging.warning("Failed to calc end of ram and time")
                logging.exception("ram calc went wrong")
                memory_consumed = None
                time_consumed = None
                pass
            
            print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
            # print(f'#########Files got are: {files}')
            return jsonify({"flag": True, "data": files_, "pagination":pagination,"displayed_columns":displayed_columns,"dropdown_columns": dropdown_dict})
        
        else:
            try:
                start_point = data['start'] - 1
                end_point = data['end']
                offset = end_point - start_point
            except:
                start_point = 0
                end_point = 20
                offset = 20
            
            group_db = DB("group_access", **db_config)
            user_groups = get_group_ids(user_name, group_db)
            print(f"##### user_groups: {user_groups}")
            
            if master_table_name:
                try:
                    database, table_name = master_table_name.split('.')
                except:
                    traceback.print_exc()
                    message = f"Master table name is in incorrect format. Check configuration."
                    return jsonify({"flag": False, "message": message})
            else:
                tables_df = extraction_db.execute_(f"SELECT * FROM `master_upload_tables`")

                if not tables_df.empty:
                    tables_list = list(tables_df["table_name"].unique())
                    tables_edit=list(tables_df["editable"])
                    tables_enable = list(tables_df["enable"])
                    table_edit=[]
                    table_edit_modified = []
                    for i in range(len(tables_list)):
                        table_result = {"display_name":tables_list[i],"edit":tables_edit[i]}
                        table_result_modified = {"display_name":tables_list[i].split('.')[1],"edit":tables_edit[i]}
                        table_edit.append(table_result )
                        table_edit_modified.append(table_result_modified )
                    table_name = tables_list[0]
                    table_enable = tables_enable[0]
                    database, table_name = tables_list[0].split('.')
                else:
                    traceback.print_exc()
                    message = f"No tables in extraction database"
                    return jsonify({"flag": False, "message" : message})
            try:
                table_db = DB(database, **db_config)
                qry=f"SELECT * FROM `{table_name}` OFFSET {start_point} ROWS FETCH NEXT 20 ROWS ONLY"
                data_frame = table_db.master_execute_(qry)
                data_frame = data_frame.astype(str)
                data_frame = data_frame.replace('NaN','')
                data_frame = data_frame.replace('nan','')
                table_name_updated = database+'.'+table_name
                query = f"SELECT `enable` from master_upload_tables where table_name='{table_name_updated}'"
                table_enable = extraction_db.execute_(query)['ENABLE'][0]
                print(f"#####TABLE ENABLE IS {table_enable}")
                print(f"####  DATA FRAME IS {data_frame}")
                header_list=data_frame.columns
                print(f"### HEADER LIST IS {header_list}")
                columns_list = list(data_frame.columns)
                total_rows = list(table_db.execute_(f"SELECT COUNT(*) FROM `{table_name}`")['COUNT(*)'])[0]
                data_frame.replace(to_replace= "None", value= '', inplace= True)
                try:
                    data_frame = data_frame.sort('id')
                except:
                    pass
                dropdown_dict = {}
                if table_name=='party_master':
                    columns = ['id','party_id','party_name','relation_mgr_emp_code','relation_mgr']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                if table_name=='city_state':
                    columns = ['id','rm_mgr_code','rm_city','rm_state']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                if table_name=='component_master':
                    columns = ['id','component_code','component_name','status','component_category','component_type']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                if table_name=='age_margin_working_uat' or table_name=='age_margin_working':
                    columns = ['id','party_id','banking_type','margin_type','dp_margin_consideration','stock_statement_type','banking_share','component_name','age','margin']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    query = f'select DISTINCT(component_name) from age_margin_working_uat'
                    component_name_df = extraction_db.execute_(query)
                    component_name_list = component_name_df['COMPONENT_NAME'].to_list()
                    dropdown_dict = {}
                    dropdown_dict['component_name']=component_name_list
                if table_name=='wbo_region':
                    columns = ['id','rm_state','wbo_region']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                data_dict = data_frame.to_dict(orient= 'records')
            except:
                traceback.print_exc()
                message = f"Could not load {table_name} from {database}"
                return jsonify({"flag": False, "message" : message})
            
            if end_point > total_rows:
                end_point = total_rows
            if start_point == 1:
                pass
            else:
                start_point += 1
            
            pagination = {"start": start_point, "end": end_point, "total": total_rows}
            print(f'Data Dict is: {data_dict}')
            
            data = {
                "header": list(data_frame.columns),
                "rowData": data_dict,
                "pagination": pagination
            }
            
            button_options_data=button_options(extraction_db)
            if button_options_data['flag']==True:
                options_data=button_options_data["options_data"]
            else:
                message=f"unable to load options data"
                options_data = {}
            
            #updation details
            try:
                last_updated = f"select `last_updated` from `master_upload_tables` where `table_name`= '{master_table_name}' LIMIT 1"
                last_updated = extraction_db.execute_(last_updated).to_dict()
                print(f"updated time by :{last_updated}")
                last_updated_by = f"select `last_updated_by` from `master_upload_tables` where `table_name`= '{master_table_name}' LIMIT 1"
                last_updated_by = extraction_db.execute_(last_updated_by).to_dict()
                print(f"last updated by :{last_updated_by}")
            except:
                last_updated = None
                last_updated_by = None

            if master_table_name:
                to_return = {
                    'flag': True,
                    'data': {
                        'data': data,
                        'options_data':options_data,
                        'enable': str(table_enable),
                        'dropdown_columns': dropdown_dict
                        },
                    'last_updated_by':last_updated_by,
                    'last_updated':last_updated
                    }
            else:        
                to_return = {
                    'flag': True,
                    'data': {
                        'master_data': [{"display_name":"New Table","edit":0}] + table_edit,
                        'master_data_modified': [{"display_name":"New Table","edit":0}] + table_edit_modified,
                        'data': data,
                        'options_data':options_data,
                        'enable': str(table_enable),
                        'dropdown_columns': dropdown_dict
                        },
                    'last_updated_by':last_updated_by,
                    'last_updated':last_updated
                    }
            
            try:
                memory_after = measure_memory_usage()
                memory_consumed = (memory_after - memory_before) / \
                    (1024 * 1024 * 1024)
                end_time = tt()
                time_consumed = str(end_time-start_time)
            except:
                logging.warning("Failed to calc end of ram and time")
                logging.exception("ram calc went wrong")
                memory_consumed = None
                time_consumed = None
                pass
            
            print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
            return to_return
    
    

