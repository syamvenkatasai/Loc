
import json
import requests
import traceback
import os
import sqlalchemy
import pandas as pd
import psutil
import jwt
import re

from db_utils import DB
from flask import Flask, request, jsonify
from time import time as tt
from sqlalchemy.orm import sessionmaker
from hashlib import sha256
from elasticsearch_utils import elasticsearch_search
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from ace_logger import Logging
from app import app
from datetime import datetime,timedelta

try:
    from app.isac_user_management import *
except:
    from isac_user_management import *
import pytz
tmzone = 'Asia/Kolkata'
import random
import string


logging = Logging()

db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD']
}

def http_transport(encoded_span):
    body = encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )

def generate_isac_ticket_no(isac_id,operation,group_access_db,user_name,changed_by,result):

    isac_data={"isac_ticket_no":isac_id,"operation":operation,"username":user_name,"changed_by":changed_by,"isac_response":json.dumps(result)}
    group_access_db.insert_dict(isac_data,'isac_request_response')
    return {'flag':True}

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes


def generate_token(apiSecret):
    payload = {
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1),  # Token valid for 1 hour
        'key': os.environ['SECRET_API']
    }
    token = jwt.encode(payload, apiSecret, algorithm='HS256')
    return token

def field_validation(tenant_id,group_access_db,row_data,isac_token):

    user_field_data_qry=f"select isac_unique_name,mandatory,pattern,pattern_msg,max_length from field_definition where isac_unique_name is not null"
    field_data_df=group_access_db.execute_(user_field_data_qry)

    field_data_df=field_data_df.to_dict(orient='records')
    
    field_definition = {}
    for item in field_data_df:
        print(f"item is {item}")
        field_definition[item['isac_unique_name']] = {
            "pattern": item['pattern'] if item['pattern'] else r".*",
            "pattern_msg": item['pattern_msg'] if item['pattern_msg'] else None,
            "max_length": item['max_length'] if item['max_length'] else 0,
            "mandatory": item['mandatory'] if item['mandatory'] else 0
        }


    for field, rules in field_definition.items():
        value = row_data.get(field, "")
        logging.info(f"### FIeld is {field} & Value is {value}")
        if field == 'branchCode' and value:
            if value and not re.match(rules["pattern"], value):
                return {"flag": False, "errorCode": 6, "errorMessage": f'Invalid branch code', "isacTicketNo": isac_token}
        if field == 'emailId' and value:
            #pattern='^.+@(hdfcbank\.com|hdfcbank\.co\.in|in\.hdfcbank\.com)$'
            pattern='^.+@(hdfcbank\.com|hdfcbank\.co\.in|in\.hdfcbank\.com|hdfcsales\.co\.in|hdfcsales\.com)$'
            if value and not re.match(pattern, value):
                return {"flag": False, "errorCode": 7, "errorMessage": f'Invalid EmailId', "isacTicketNo": isac_token}   
        if not value and rules['mandatory']:
            return {"flag": False, "errorCode": 5, "errorMessage": f'{field} is mandatory', "isacTicketNo": isac_token}
        if value and field!='emailId':
            if not re.match(rules["pattern"], value):
                return {"flag": False,"errorCode":5, "errorMessage" : f'Invalid field data for {field}',"isacTicketNo":isac_token}
            if len(value) > rules["max_length"]:
                return {"flag": False,"errorCode":5, "errorMessage" : f'Invalid field data for {field}',"isacTicketNo":isac_token}
        else:
            continue
    return {"flag": True,"errorCode":5, "errorMessage" : f'All Fields are valid',"isacTicketNo":isac_token}

def field_validation2(tenant_id,group_access_db,row_data,isac_token):

    user_field_data_qry=f"select isac_unique_name,mandatory,pattern,pattern_msg,max_length from field_definition where isac_unique_name is not null"
    field_data_df=group_access_db.execute_(user_field_data_qry)

    field_data_df=field_data_df.to_dict(orient='records')
    
    field_definition = {}
    for item in field_data_df:
        print(f"item is {item}")
        field_definition[item['isac_unique_name']] = {
            "pattern": item['pattern'] if item['pattern'] else r".*",
            "pattern_msg": item['pattern_msg'] if item['pattern_msg'] else None,
            "max_length": item['max_length'] if item['max_length'] else 0,
            "mandatory": item['mandatory'] if item['mandatory'] else 0
        }


    for field, rules in field_definition.items():
        value = row_data.get(field, "")
        if field in ("branchName","employeeName","employeeCode","branchCode","departmentCode","departmentName","emailId") and value == "":
            continue
        logging.info(f"### FIeld is {field} & Value is {value}")
        if field == 'branchCode' and value:
            if value and not re.match(rules["pattern"], value):
                return {"flag": False, "errorCode": 6, "errorMessage": f'Invalid branch code', "isacTicketNo": isac_token}
        if field == 'emailId' and value:
            #pattern='^.+@(hdfcbank\.com|hdfcbank\.co\.in|in\.hdfcbank\.com)$'
            pattern='^.+@(hdfcbank\.com|hdfcbank\.co\.in|in\.hdfcbank\.com|hdfcsales\.co\.in|hdfcsales\.com)$'
            if value and not re.match(pattern, value):
                return {"flag": False, "errorCode": 7, "errorMessage": f'Invalid EmailId', "isacTicketNo": isac_token}   
        if not value and rules['mandatory']:
            return {"flag": False, "errorCode": 5, "errorMessage": f'{field} is mandatory', "isacTicketNo": isac_token}
        if value and field!='emailId':
            if not re.match(rules["pattern"], value):
                return {"flag": False,"errorCode":5, "errorMessage" : f'Invalid field data for {field}',"isacTicketNo":isac_token}
            if len(value) > rules["max_length"]:
                return {"flag": False,"errorCode":5, "errorMessage" : f'Invalid field data for {field}',"isacTicketNo":isac_token}
        else:
            continue
    return {"flag": True,"errorCode":5, "errorMessage" : f'All Fields are valid',"isacTicketNo":isac_token}


def decode_generated_token(token,secret_key):
    try:
        secret_key=os.environ['SECRET_KEY']
        decoded_payload = jwt.decode(token, secret_key, algorithms=['HS256'])
        return decoded_payload
    except jwt.ExpiredSignatureError:
        return "Token has expired"
    except jwt.InvalidTokenError:
        return "Invalid token"
    except:
        return False
    
@app.route('/change_isac_user_status', methods=['POST', 'GET'])
def change_isac_user_status():

    try:

        headers=request.headers
        headers_dict={}


        headers=request.headers
        for k,v in headers.items():
            logging.info(f"## Header key Val is {k}:{v}")
            headers_dict[k]=v
        logging.info(f"## Headers got are {headers}")
        bearer_token = headers_dict.get('Authorization',None)
        secret_key=headers_dict.get('apiKey',None)
        
        logging.info(f"### Bearer got is {bearer_token}")
        if bearer_token:
            token=bearer_token.split(" ")[2]
            logging.info(f" ## Token got is {token}")
            token_response=decode_generated_token(token,secret_key)
            logging.info(f"## token response got is {token_response}")
        else:
            pass
    except Exception as e:
        logging.info(f"## Exception occured ..{e}")
        pass
    data = request.json
    logging.info(f"Request Data: {data}")
    

    try:
        isac_result = check_isac_token(data)
        if isac_result:
            return isac_result

        else:
            pass
        print(f"Result from check_isac_token: {isac_result}")
    except Exception as e:
        logging.exception(f"## Exception occured while checking isac token..{e}")
        pass

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', os.environ['TENANT_ID'])
    user = data.get('initiatedBy', None)
    initiated_by = data.get('initiatedBy', None)
    approved_by = data.get('approvedBy', None)
    session_id = data.get('session_id', None)
    
    trace_id = generate_random_64bit_string()
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='user_auth_api',
        span_name='change_isac_user_status',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
        try:
            username = data.get('userId', None)
            print(f"username is {username}")
            if username:
                username=username.lower()
            status = data.pop('activity', 'enable').lower()
            isac_token=data.get('isacTicketNo',None)

            logging.info(f'###status  {status}')
            
            db_config['tenant_id'] = tenant_id

            db = DB('group_access', **db_config)
            
            try:
                result1 = check_isac_token(data)
                print(f"Result from check_isac_token: {result1}")
            except Exception as e:
                logging.exception(f"## Exception occured while checking isac token..{e}")
                pass

            data_active_directory_query = f"""
                                            SELECT USERNAME,
                                                CASE 
                                                    WHEN USERNAME = '{username}' THEN STATUS
                                                    ELSE NULL
                                                END AS STATUS,ID,PREVIOUS_STATUS
                                            FROM active_directory
                                            """
            data_active_directory_df = db.execute_(data_active_directory_query)
            usernames = list(data_active_directory_df['USERNAME'])
            usernames_list = [elem.lower() for elem in usernames if elem is not None]
            user_status = data_active_directory_df.loc[data_active_directory_df['USERNAME'] == username, 'STATUS'].values[0] if username in data_active_directory_df['USERNAME'].values else None
            user_id=data_active_directory_df.loc[data_active_directory_df['USERNAME'] == username, 'ID'].values[0] if username in data_active_directory_df['USERNAME'].values else None
            previous_status=data_active_directory_df.loc[data_active_directory_df['USERNAME'] == username, 'PREVIOUS_STATUS'].values[0] if username in data_active_directory_df['USERNAME'].values else '[]'
            try:
                previous_status=json.loads(previous_status)
            except Exception as e:
                previous_status=[]
            logging.info(f'#######user_status is = {user_status} \n  usernames_list = {usernames_list} \n ID IS {user_id} \n previous is {previous_status}'  )

            print(f"user_status is {user_status}")


            if username.lower() not in usernames_list or user_id is None:
                message=f"User id Doesn't exist"
                response_data = {'errorCode':2,'isacTicketNo':isac_token,'errorMessage': message}
            elif status=='unlock' and user_status in ('dormant','disable'):
                message=f"User id not locked"
                response_data = {'errorCode':12,'isacTicketNo':isac_token,'errorMessage': message}
            elif status=='unlock' and user_status=='dormant':
                message=f"User id is in Dormant status"
                response_data = {'errorCode':14,'isacTicketNo':isac_token,'errorMessage': message}
            elif status=='enable' and user_status=='lock':
                message=f"User id is in enable status"
                response_data = {'errorCode':8,'isacTicketNo':isac_token,'errorMessage': message}
            elif status=='unlock' and user_status =='disable':
                message=f"User id is in disable status"
                response_data = {'errorCode':9,'isacTicketNo':isac_token,'errorMessage': message}
            elif status=='unlock' and user_status in ('unlock','dormant'):
                message=f"User id not locked"
                response_data = {'errorCode':12,'isacTicketNo':isac_token,'errorMessage': message}
            elif status=='enable' and user_status=='lock':
                message=f"User id is in locked State"
                response_data = {'errorCode':12,'isacTicketNo':isac_token,'errorMessage': message}
            elif status in ['delete','enable','unlock'] and user_status == 'delete':
                message=f"User id already deleted"
                response_data = {'errorCode':10,'isacTicketNo':isac_token, 'errorMessage': message}
            elif status == 'disable' and user_status == 'disable':
                message=f"User id already in disabled status"
                response_data = { 'errorCode':9,'isacTicketNo':isac_token,'errorMessage': message}
            elif (status == 'enable' and user_status == 'enable') or (status == 'revoke' and user_status == 'revoke'):
                message=f"User id already in enable status"
                response_data = {'errorCode':8,'isacTicketNo':isac_token, 'errorMessage': message}
            elif status == 'unlock' and user_status == 'delete':
                message=f"User id already deleted can not be modified"
                response_data = {'errorCode':13,'isacTicketNo':isac_token, 'errorMessage': message}
            elif status == 'unlock' and user_status in ('unlock','disable','dormant','enable','revoke','enable'):
                message=f"User id not locked"
                response_data = {'errorCode':12,'isacTicketNo':isac_token, 'errorMessage': message}
            elif status == 'revoke' and user_status in ('enable','disable','lock','unlock','dormant'):
                message=f"User id is in active status"
                response_data = {'errorCode':8,'isacTicketNo':isac_token, 'errorMessage': message}
            elif status == 'unlock' and user_status == 'disable':
                message=f"User id already deleted can not be modified"
                response_data = {'errorCode':13,'isacTicketNo':isac_token, 'errorMessage': message}
            elif status == 'unlock' and user_status != 'lock':
                message=f"User id not locked"
                response_data = {'errorCode':12,'isacTicketNo':isac_token, 'errorMessage': message}

        
            else:
                #User deleted date & User disabled date adding
                current_ist = datetime.now(pytz.timezone(tmzone))
                currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
                current_date = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                if status == 'delete':
                    update = f"""update active_directory set status = '{status}' , DELETED_DATE= TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS'), USER_DISABLED_DATE= null,approved_by='{approved_by}', 
                                MAKER_ID = '{initiated_by}', MAKER_NAME = '{initiated_by}', MAKER_DATE = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS'), CHECKER_ID = '{approved_by}', CHECKER_NAME = '{approved_by}', CHECKER_DATE = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS')
                                where username = '{username}'"""
                elif status == 'disable':
                    update = f"""update active_directory set status = '{status}' , DELETED_DATE= TO_TIMESTAMP('2049-DEC-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), USER_DISABLED_DATE= TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS'),approved_by='{approved_by}',
                                MAKER_ID = '{initiated_by}', MAKER_NAME = '{initiated_by}', MAKER_DATE = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS'), CHECKER_ID = '{approved_by}', CHECKER_NAME = '{approved_by}', CHECKER_DATE = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS') 
                                where username = '{username}'"""
                else:
                    #Whenever the status is unlock then the status in the active directory need as enable
                    status_change = status
                    if status == 'unlock' or status == 'revoke':
                        status_change = 'enable'
                    update = f"""update active_directory set status = '{status_change}' , DELETED_DATE= TO_TIMESTAMP('2049-DEC-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), USER_DISABLED_DATE= null ,approved_by='{approved_by}', login_attempts = 3,
                                MAKER_ID = '{initiated_by}', MAKER_NAME = '{initiated_by}', MAKER_DATE = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS'), CHECKER_ID = '{approved_by}', CHECKER_NAME = '{approved_by}', CHECKER_DATE = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS')
                                where username = '{username}'"""
                logging.info(f'##update {update}')
                db.execute(update)
                
                role_select_query = f"SELECT EMPLOYEE_NAME,BRANCH_CODE,BRANCH_NAME,ROLE,OLD_EMPLOYEE_NAME,OLD_ROLE_NAME,OLD_BRANCH_NAME FROM USER_OPERATIONS WHERE USERNAME = '{username}' ORDER BY user_modified_date DESC FETCH FIRST 1 ROWS ONLY"
                role_select_query_data = db.execute_(role_select_query)
                if role_select_query_data.empty:
                    logging.info("####role_select_query_data is empty. if block executing")
                    role_select_query = f"SELECT EMPLOYEE_NAME,BRANCH_CODE,BRANCH_NAME,ROLE,OLD_EMPLOYEE_NAME,OLD_ROLE_NAME,OLD_BRANCH_NAME FROM active_directory WHERE USERNAME = '{username}'"
                    role_select_query_data = db.execute_(role_select_query)
                logging.info(f"####role_select_query_data is = {role_select_query_data}")

                user_old_role = role_select_query_data['ROLE'].iloc[0]
                if not user_old_role or user_old_role == "None" or user_old_role == None:
                    user_old_role = role_select_query_data['OLD_ROLE_NAME'].iloc[0]
                

                #Excluding the old employee_name, role and branch from the activity report
                data_to_insert = {
                    'ID':str(user_id),
                    'USERNAME': username,
                    'EMPLOYEE_NAME': role_select_query_data['EMPLOYEE_NAME'].iloc[0],
                    'BRANCH_CODE': role_select_query_data['BRANCH_CODE'].iloc[0],
                    'BRANCH_NAME': role_select_query_data['BRANCH_NAME'].iloc[0],
                    'ROLE': user_old_role,
                    'ACTIVITY': status,
                    'STATUS':status,
                    'MAKER_ID': initiated_by,
                    'CREATED_USER': initiated_by,
                    'USER_MODIFIED_DATE': current_date,
                    'CHECKER_ID': approved_by,
                    'CHECKER_NAME': approved_by,
                    'CHECKER_DATE': current_date,
                    'LAST_UPDATED_DATE': current_date
                }

                filtered_data = {k: v for k, v in data_to_insert.items() if v != ''}
                columns_list = ', '.join(filtered_data.keys())
                #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
                values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())
                
                insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
                logging.info(f'#####insert_query is {insert_query}')
                query_result = db.execute_(insert_query)
                logging.info(f'#####query_result is {query_result}')

                response_data={'errorCode':0,'isacTicketNo':isac_token,'errorMessage':'Success'}
                
        except Exception as e:
            message = 'error in changing the user status'
            logging.info(f"## Exception occured ..{e}")
            response_data =  {'errorCode':2,'isacTicketNo':isac_token,'errorMessage':'Invalid User ID'}
        
        try:
            ##update the last req new in with current time if status changed from dormant to enable
            if user_status=='dormant' and status=='enable':
                update_live_session = f"""update live_sessions set LAST_REQUEST_NEW = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS') where user_='{username}'"""
                db.execute_(update_live_session)
                previous_status.append("dormant")
                update_qry = f"""update active_directory set previous_status = '{json.dumps(previous_status)}' , last_updated = TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS') where username='{username}'"""
                db.execute_(update_qry)

        except Exception as e:
            logging.exception(f"## Exception occured while updating current timestamp..{e}")

        
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            time_consumed = str(end_time-start_time)
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        logging.info(f'###response_data is {response_data}')
        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": username,
                        "api_service": "change_status", "service_container": "user_auth_api", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(response_data['errorMessage']), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['errorCode'])}
        insert_into_audit(audit_data)

        try:
            res=generate_isac_ticket_no(isac_token,status,db,username,user,response_data)
        except Exception as e:
            logging.exception(f"## Exception while inserting Isac token..{e}")
            pass

        return jsonify(response_data)

def check_isac_token(data):
    print(f"Data received: {data}") 
    isac_token = data.get("isacTicketNo", None)
    activity = data.get("activity", None)
    if activity:
        activity=activity.lower()
    tenant_id = data.pop('tenant_id', os.environ['TENANT_ID'])
    db_config['tenant_id'] = tenant_id
    group_access_db = DB('group_access', **db_config)

    if not isac_token:
        return {"flag": False, "errorCode": 5, "message": 'Missing isacTicketNo'}

    isac_token_query = f"SELECT * FROM ISAC_REQUEST_RESPONSE WHERE ISAC_TICKET_NO='{isac_token}'"
    token_result = group_access_db.execute_(isac_token_query)
    print(f"token_result is {token_result}")
    if not token_result.empty:
        token_result_msg = json.loads(list(token_result['isac_response'])[0])
        return token_result_msg
    else:
        return False

@app.route('/modify_user_isac', methods=['GET','POST'])
def modify_user_isac():

    try:

        headers=request.headers
        headers_dict={}

        headers=request.headers
        for k,v in headers.items():
            logging.info(f"## Header key Val is {k}:{v}")
            headers_dict[k]=v
        logging.info(f"## Headers got are {headers}")
        bearer_token = headers_dict.get('Authorization',None)
        secret_key=headers_dict.get('apiKey',None)
        
        logging.info(f"### Bearer got is {bearer_token}")
        if bearer_token:
            token=bearer_token.split(" ")[1]
            logging.info(f" ## Token got is {token}")
            token_response=decode_generated_token(token,secret_key)
            logging.info(f"## token response got is {token_response}")
        else:
            pass
    except Exception as e:
        logging.info(f"## Exception occured ..{e}")
        pass
    data = request.json
    logging.info(f'Request data in modify_user: {data}')
    req_data_cpy=data.copy()
    session_id = data.get('session_id', None)
    sources=data.pop('sources',{"role": "user"})
    tenant_id=data.pop('tenant_id',os.environ['TENANT_ID'])
    route_name=data.pop('route_name',None)
    user=data.get('initiatedBy',None)
    session_id=data.pop('session_id',None)
    approved_by=data.get('approvedBy',None)
    isac_token=data.get("isacTicketNo",None)
    activity=data.get("activity",None)
    role=data.get("role",None)
    attributes=[]
    if "role" in data:
        attributes.append({"role":data['role']})
    data['attributes']=attributes
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    try:
        operation = data.get('activity').lower()
        logging.info(f"####operation is {operation}")
        user_name=data.get('userId',None)
        employee_name=data.get('employeeName',None)
        user_email=data.get('emailId',None)
        if user_name:
            user_name = user_name.lower()
            data['userId'] = data['userId'].lower()
        
        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value
        
        if employee_name:
            employee_name=escape_apostrophe(employee_name)
            data['employeeName'] = employee_name
        if user_email:
            user_email=escape_apostrophe(user_email)
            data['emailId'] = user_email

        print(f" ### employee GOT IS {employee_name}")
    except:
        message = "Received unexpected request data."
        result={"flag": False, "message" : message}
    
    trace_id = generate_random_64bit_string()
    attr = ZipkinAttrs(
            trace_id=trace_id,
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='user_management',
            zipkin_attrs=attr,
            span_name='modify_user_isac',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        queue_db = DB('queues', **db_config)

        try:
            isac_result = check_isac_token(data)
            if isac_result:
                return isac_result

            else:
                pass
            print(f"Result from check_isac_token: {isac_result}")
        except Exception as e:
            logging.exception(f"## Exception occured while checking isac token..{e}")
            pass
        try:
            if operation == 'edit':
                field_response=field_validation2(tenant_id,group_access_db,data,isac_token)
            else:
                field_response=field_validation(tenant_id,group_access_db,data,isac_token)
            if not field_response['flag']:
                return field_response
            else:
                pass
        except Exception as e:
            logging.exception(f"FIELD VALIDATION ISSUE OCCURED..{e}")
            return {"errorCode":5, "errorMessage" : f'Invalid field data',"isacTicketNo":isac_token}


        if operation == 'edit':
            try:
                get_id=f"select id from active_directory where username='{user_name}'"
                user_id=group_access_db.execute_(get_id)['id'].to_list()[0]
                result = edit_user_isac(user_id,data, group_access_db, user,isac_token)
            except Exception as e:
                logging.exception(f"Unable to fetch the user id")
                return {"errorCode":2, "errorMessage" :"Invalid User ID","isacTicketNo":isac_token}           
        elif operation == 'create':
            result = create_user_isac(data, group_access_db, queue_db, user,isac_token)
        else:
            # result = {'message':'Didnot receive proper operator'}
            logging.info(f"Did not receive proper operator !!!")
            return {"errorCode":2, "errorMessage" :"Invalid User ID","isacTicketNo":isac_token}           
            
        
        headers = {'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}

        requests.post('https://queueapi:443/clear_cache', headers=headers,verify=False)

        return_data = result
            
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

        logging.info(f" #### um info return data got is {return_data}")
        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user_name,
                        "api_service": "modify_user", "service_container": "user_management", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": str(return_data['errorMessage']), "trace_id": trace_id, "session_id": session_id,"status":str(return_data['errorCode'])}
        insert_into_audit(audit_data)

        try:
            res=generate_isac_ticket_no(isac_token,operation,group_access_db,user_name,user,result)
        except Exception as e:
            logging.exception(f"## Exception while inserting Isac token..{e}")
            pass

        return jsonify(result)



def insert_into_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True

def edit_user(user_id,row_data, group_access_db, user,isac_token, changed_data, approved_by, initiated_by):

    try:
        user_id=user_id
        edited_data = changed_data
        user_name = row_data['username']
        attributes = row_data.get('attributes', [])
        approved_by = approved_by
        initiated_by = initiated_by
    except:
        traceback.print_exc()
        return {"flag": False,"errorCode":11, "message" : 'User id not deleted',"isacTicketNo":isac_token}   

        
    engine = group_access_db.engine
    Session = sessionmaker(bind = engine)
    session = Session()
    g_db_no_autocommit = group_access_db
    
    try:

        if not edited_data:
            return {"flag": False,"errorCode":10, "message" : 'Nothing to Update',"isacTicketNo":isac_token}

        query_ = f"SELECT COLUMN_NAME FROM all_tab_columns WHERE table_name = 'ACTIVE_DIRECTORY'"
        query_result = group_access_db.execute_(query_)
        column_names = list(query_result['COLUMN_NAME'])
        logging.info(f'###column_names {column_names}')

        logging.info(f'###user id is {user_id}')
        logging.info(f'###row data is {row_data}')

        data_active_directory_query = f"select EMPLOYEE_ID, EMPLOYEE_CODE, EMPLOYEE_NAME, USER_EMAIL, USERNAME, ROLE from active_directory where ID <> '{user_id}' AND status <> 'rejected'"
        data_active_directory_df = group_access_db.execute_(data_active_directory_query)
        logging.info(f'#######data_active_directory_df = {data_active_directory_df}')

        employee_codes = list(data_active_directory_df['EMPLOYEE_CODE'])
        employee_codes = [elem.lower() if elem is not None else None for elem in employee_codes]
        employee_names = list(data_active_directory_df['EMPLOYEE_NAME'])
        employee_names = [elem.lower() if elem is not None else None for elem in employee_names]
        user_emails = list(data_active_directory_df['USER_EMAIL'])
        user_emails = [elem.lower() if elem is not None else None for elem in user_emails]
        usernames = list(data_active_directory_df['USERNAME'])
        usernames = [elem.lower() if elem is not None else None for elem in usernames]
        roles = list(data_active_directory_df['ROLE'])
        roles = [elem.lower() if elem is not None else None for elem in roles]
        logging.info(f'####employee_codes = {employee_codes},employee_names = {employee_names}, user_emails = {user_emails}, usernames = {usernames}, roles = {roles}')

        data_active_directory_query = f"select USERNAME from active_directory"
        data_active_directory_df = group_access_db.execute_(data_active_directory_query)
        usernames_list = list(data_active_directory_df['USERNAME'])
        usernames_list = [elem.lower() for elem in usernames_list if elem is not None]
        logging.info(f'#######user_name is {user_name} , data_active_directory_df = {data_active_directory_df}')
        if user_name.lower() not in usernames_list:
            return {"flag": False,"errorCode":10, "message" : 'User does not exist',"isacTicketNo":isac_token}   

        res = ''
        try:
            if row_data['employee_code'].lower() in employee_codes:
                res = res+'employee_code '
            if row_data['employee_name'].lower() in employee_names:
                res = res+'employee_name '
            if row_data['user_email'].lower() in user_emails:
                res = res+'user_email '
            if row_data['username'].lower() in usernames:
                res = res+'user_id '
        except Exception as e:
            logging.exception(f"Exception Occured..{e}")
            pass
        logging.info(f'##res = {res}')
        if res != '':
            res = 'Duplicate '+res.replace(' ', ', ').strip(', ')
            message = res
            return {"flag": False,"errorCode":3, "message" : message,"isacTicketNo":isac_token}   

        set_clause_arr = []

        prev_data_query = f"SELECT USER_AUDIT,EMPLOYEE_NAME, ROLE, BRANCH_NAME, OLD_EMPLOYEE_NAME, OLD_ROLE_NAME, OLD_BRANCH_NAME FROM active_directory WHERE id = '{user_id}'"
        prev_data = group_access_db.execute(prev_data_query)
        logging.info(f'####prev_data {prev_data}')
        prev_employee_name = list(prev_data['employee_name'])[0]
        prev_branch_name = list(prev_data['branch_name'])[0]
        prev_role = list(prev_data['role'])[0]
        audit_data=list(prev_data['old_role_name'])[0]
    
        old_employee_name = ''
        old_branch_name = ''
        old_role_name = ''
        new_employee_name = ''
        new_branch_name = ''
        new_role_name = ''


        #row data
        role_name = row_data['attributes'][0]['role']
        branch_name = row_data['branch_name']
        employee_name = row_data['employee_name']

        if employee_name != prev_employee_name:
            old_employee_name = prev_employee_name
            new_employee_name = employee_name
        if branch_name != prev_branch_name:
            old_branch_name = prev_branch_name
            new_branch_name = branch_name

        if role_name != prev_role:
            old_role_name = prev_role
            new_role_name = role_name
    

        logging.info(f'old_employee_name={old_employee_name}, old_branch_name={old_branch_name}, old_role_name={old_role_name}')
        
        #Handling the apostrophy
        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value
        
        if old_employee_name:
            old_employee_name=escape_apostrophe(old_employee_name)
        
        old_details = []
        
        changed_keys = []
        for key,value in edited_data.items():
            changed_keys.append(key)
        query = f"select * from active_directory where username='{user_name}'"
        res = group_access_db.execute_(query)
        created_date = res['created_date'][0]
        user_audit = res['user_audit'][0]
        user_id = res['id'][0]
        final_list = []
        print(f'User audit is: {user_audit}')
        res = res.to_dict(orient='records')
        print(res)
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')

        created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')

        l_list = []
        final_dict = {}
        print(changed_keys)
        for i in changed_keys:
            res_key = i+'_'+'previous'
            res[0][i] = escape_apostrophe(res[0][i])
            final_dict[res_key] = res[0][i]
        print(final_dict)
        print(edited_data)
        l = []
        for key,value in edited_data.items():
            dic = {}
            key_ = key+'_'+'previous'
            dic['user_id'] = str(user_id)
            dic['field_name'] = key.replace('_',' ').title()
            dic['old_value'] = final_dict[key_]
            dic['new_value'] = value
            dic['modified_date'] = currentTS
            dic['created_date'] = created_date
            dic['modified_by'] = initiated_by
            l.append(dic)
            
        l_list = l
        final_list = l_list

        final_list = json.dumps(final_list)

        print(final_list)

        query = f"update active_directory set user_audit='{final_list}' where username='{user_name}'"
        print(query)
        group_access_db.execute_(query)
        

        #for UAM Maker and UAM Checker
        if old_employee_name and old_employee_name not in ("None","NONE","none"):
            row_data['OLD_EMPLOYEE_NAME'] = old_employee_name
        if old_branch_name and old_branch_name not in ("None","NONE","none"):
            row_data['OLD_BRANCH_NAME'] = old_branch_name
        if old_role_name and old_role_name not in ("None","NONE","none"):
            row_data['OLD_ROLE_NAME'] = old_role_name
        try:
            del row_data['sources']
            del row_data['changed_fields']
            del row_data['initiatedBy']
            del row_data['approvedBy']
            del row_data['route_name']
        except:
            pass

        for set_column, set_value in row_data.items():
            if set_column!="attributes" and set_column not in ('tenant_id','session_id'):
                set_clause_arr.append(f"{set_column} = '{set_value}'")
            else:
                if attributes and set_column not in ('tenant_id','session_id'):
                    set_column='role'
                    set_value=attributes[0]['role']
                    set_clause_arr.append(f"{set_column} = '{set_value}'")
                else:
                    pass
        set_clause_arr.append(f"CREATED_USER = '{user}'")
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
        current_date = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

        modified_date = f"TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS')"
        #This `set_clause_arr` also used in the below for inserting
        set_clause_arr.append(f"USER_MODIFIED_DATE = {modified_date}")

        maker_username = initiated_by
        maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
        maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
        maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
        logging.info(f"####maker_checker_employee_names is {maker_checker_ids}")
        maker_name = maker_checker_ids[0]
        logging.info(f"maker_name is {maker_name}")

        maker_name = escape_apostrophe(maker_name)
        #Adding maker details to the acti_directory_modifications table
        set_clause_arr.append(f"maker_id = '{initiated_by}'")
        set_clause_arr.append(f"maker_name = '{maker_name}'")
        set_clause_arr.append(f"maker_date = '{current_date}'")
        set_clause_arr.append(f"last_updated = '{current_date}'")


        set_clause_arr = [clause for clause in set_clause_arr if not clause.endswith("'None'")]
        logging.info(f"###set_clause_arr is {set_clause_arr}")
        set_clause_string = ', '.join(set_clause_arr)


        logging.info(f" SET CLAUSE STRING IS {set_clause_string}")

        #updating for UAM Maker and UAM Checker 
        active_users_query = f"SELECT USERNAME FROM active_directory_modifications where STATUS NOT IN ('approved','rejected')"
        data_active_directory_df = group_access_db.execute_(active_users_query)
        usernames_list = list(data_active_directory_df['USERNAME'])
        usernames_list = [elem.lower() for elem in usernames_list if elem is not None]
        if user_name.lower() in usernames_list:
            return {"flag": False,"errorCode":2, "message" : 'Record already sent for verification',"isacTicketNo":isac_token}
        else:
            
            columns = ", ".join([clause.split("=")[0].strip() for clause in set_clause_arr if clause.split("=")[0].strip() != "id"])
            values = ", ".join([clause.split("=")[1].strip().replace("'", '"') if clause.split("=")[1].strip().startswith("{") and clause.split("=")[1].strip().endswith("}") else clause.split("=")[1].strip() for clause in set_clause_arr if clause.split("=")[0].strip() != "id"])
            print(f'Columns are: {columns}')
            print(f'Values are: {values}')
            query = f"INSERT INTO active_directory_modifications ({columns}) VALUES ({values})"

        logging.info(f" SET CLAUSE STRING query IS {query}")
        result = group_access_db.execute_(query)
        logging.info(f'result-----------{result}')

        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

        maker_username = initiated_by
        maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
        maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
        maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
        logging.info(f"####maker_checker_ids is {maker_checker_ids}")
        maker_name = maker_checker_ids[0]
        logging.info(f"maker_name is {maker_name}")
        

        data_to_insert = {
            'ID': str(user_id),
            'USERNAME': row_data['username'],
            'EMPLOYEE_NAME': row_data['employee_name'],
            'BRANCH_CODE': row_data['branch_code'],
            'BRANCH_NAME': row_data['branch_name'],
            'ROLE': row_data['attributes'][0]['role'],
            'ACTIVITY': 'Modify',
            'STATUS': 'Modify',
            'MAKER_ID': initiated_by,
            'CREATED_USER': maker_name,
            'USER_MODIFIED_DATE': currentTS,
            'OLD_EMPLOYEE_NAME': old_employee_name,
            'OLD_ROLE_NAME': old_role_name,
            'OLD_BRANCH_NAME': old_branch_name,
            'LAST_UPDATED_DATE': currentTS,
            'NEW_EMPLOYEE_NAME': new_employee_name,
            'NEW_ROLE_NAME': new_role_name,
            'NEW_BRANCH_NAME': new_branch_name
        }

        filtered_data = {k: v for k, v in data_to_insert.items() if v != ''}
        columns_list = ', '.join(filtered_data.keys())
        #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
        values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())

        insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
        try:
            update_data = group_access_db.execute(insert_query)
            logging.info(f'update_data result----> {update_data}')
        except Exception as e:
            logging.error(f'Error executing insert query: {e}')


        if not result:
            session.rollback()
            logging.warning('rolling back')
            session.close()
            logging.warning('closing session')
            return {"flag": False,"errorCode":2, "message" : 'Unable to update user',"isacTicketNo":isac_token}  
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":2, "message" : 'Unable to update user',"isacTicketNo":isac_token}  
    
    try:
        query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
        active_directory_df = group_access_db.execute_(query)
        user_id = list(active_directory_df['id'])[0]
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while fetching the user {user_name} from active directory"
        return {"flag": False, "message" : message}    
    
    try:
        query1 = f"SELECT * FROM `organisation_attributes`"
        organisation_attributes_df = group_access_db.execute_(query1)
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while fetching oraganisation attributes from database"
        return {"flag": False, "message" : message}
   
    result = generate_insert_user_org_mapping(user_id, row_data, group_access_db)
    logging.info(f"####result is {result}")
    to_insert = result['data'] if result['flag'] else []
    
    # if to_insert:
    try:
        organisation_mapping_delete_query = f"DELETE FROM user_organisation_mapping WHERE user_id = '{user_id}'" 
        result = session.execute(organisation_mapping_delete_query)
        
        if not result:
            session.rollback()
            logging.warning('rolling back')
            session.close()
            logging.warning('closing session')
            message = f"Something went wrong while deleting the user {user_name} | user_id {user_id} from user_organisation_mapping"
            return {"flag": False, "message" : message}
    
    except:
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while deleting the user {user_name} | user_id {user_id} from user_organisation_mapping"
        return {"flag": False, "message" : message}
    try:    
        insert_query = generate_multiple_insert_query(to_insert, 'user_organisation_mapping')
        result = session.execute(insert_query)
        
        if not result:
            message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping"
            return {"flag": False, "message" : message}
    except:
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping"
        return {"flag": False, "message" : message}
  
    
    session.commit()
    session.close()

    return {"flag": True,"errorCode":0, "message" : 'Modifications has been done and sent for verification',"isacTicketNo":isac_token}



def create_user(row_data, group_access_db, queue_db, user,isac_token):
    # TRY USING COMMIT AND ROLLBACK WITH SQLALCHEMY
    orig_row_data = row_data.copy()
    try:
        if user:
            row_data['CREATED_USER'] = user
        row_data['role'] = row_data['attributes'][0]['role']
        attributes = row_data.pop('attributes', [])
        user_name = row_data['username']
        initiated_by = row_data.get('initiatedBy',None)
        print(f"row_data received : {row_data}")
    except:
        traceback.print_exc()
        # message = "id not present in request data."
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    
    engine = group_access_db.engine
    g_db_no_autocommit = group_access_db
    Session = sessionmaker(bind = engine)
    session = Session()
    
    try:

        #Handling the apostrophy
        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value

        data_active_directory_query = f"select EMPLOYEE_ID, EMPLOYEE_CODE, EMPLOYEE_NAME, USER_EMAIL, USERNAME, ROLE from active_directory where status <> 'rejected'"
        data_active_directory_df = group_access_db.execute_(data_active_directory_query)
        print(f'#######data_active_directory_df = {data_active_directory_df}')
        employee_codes = list(data_active_directory_df['EMPLOYEE_CODE'])
        employee_codes = [elem.lower() if elem is not None else None for elem in employee_codes]
        employee_names = list(data_active_directory_df['EMPLOYEE_NAME'])
        employee_names = [elem.lower() if elem is not None else None for elem in employee_names]
        user_emails = list(data_active_directory_df['USER_EMAIL'])
        user_emails = [elem.lower() if elem is not None else None for elem in user_emails]
        usernames = list(data_active_directory_df['USERNAME'])
        usernames = [elem.lower() if elem is not None else None for elem in usernames]
        roles = list(data_active_directory_df['ROLE'])
        roles = [elem.lower() if elem is not None else None for elem in roles]
        print(f'####employee_codes = {employee_codes},employee_names = {employee_names}, user_emails = {user_emails}, usernames = {usernames}, roles = {roles}')

        
        res = ''
        if row_data['employee_code'] is not None and row_data['employee_code'].lower() in employee_codes:
            res = res+'Employee_code '
        if row_data['employee_name'] is not None and row_data['employee_name'].lower() in employee_names:
            res = res+'Employee_name '
        if row_data['user_email'] is not None and row_data['user_email'].lower() in user_emails:
            res = res+'User_email '
        if row_data['username'] is not None and row_data['username'].lower() in usernames:
            res = res+'User_id '
        print(f'##res = {res}')
        if res != '':
            res = res.replace(' ', ', ').strip(', ') + ' already exist'
            message = res
            logging.info(f'##res = {res}, message = {message}')
            
            return {"flag": False,"errorCode":1, "message" : message,"isacTicketNo":isac_token}
        
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
        row_data['USER_MODIFIED_DATE'] = currentTS        

        #Password for every user set to 1234 for ACE login
        user_password = '1234'
        row_data['password'] = sha256(user_password.encode()).hexdigest()
        row_data['ISAC_TICKET_NO']=isac_token

        #Assigning status as "waiting" to the newly created user
        row_data['STATUS'] = 'waiting'
        row_data['username']=user_name
        try:
            user_name=row_data.pop('user_name')
        except:
            pass
        
        query = f'select max(id) as id from active_directory'
        id_ = group_access_db.execute_(query)['id'][0]
        row_data['id'] = id_+1
        logging.info(f'Row Data is: {row_data}')

        query2 = f"select USERNAME from active_directory where status = 'rejected'"
        usernames_ = group_access_db.execute_(query2)
        usernames_ = list(usernames_['USERNAME'])
        usernames_ = [elem.lower() for elem in usernames_ if elem is not None]
        if user_name not in usernames_:

            maker_username = initiated_by
            maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
            maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
            maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
            logging.info(f"####maker_checker_employee_names is {maker_checker_ids}")
            maker_name = maker_checker_ids[0]
            logging.info(f"maker_name is {maker_name}")

            #Adding making uam maker details to row data for adding to active_directory and active_directory_modifications
            row_data['maker_id'] = initiated_by
            maker_name = escape_apostrophe(maker_name)
            row_data['maker_name'] = maker_name
            row_data['maker_date'] = currentTS

            create_user_query = generate_insert_query(row_data, 'active_directory')

            row_data['maker_id'] = initiated_by
            row_data['maker_name'] = maker_name
            row_data['maker_date'] = currentTS
            row_data['last_updated'] = currentTS

            create_user_query_2 = generate_insert_query(row_data, 'active_directory_modifications')

            print(f'create_user_query----{create_user_query}')
            print(f'create_user_query_2----{create_user_query_2}')
            session.execute(create_user_query)
            session.execute(create_user_query_2)
            session.commit()

            query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
            active_directory_df = group_access_db.execute_(query)
            user_id = list(active_directory_df['id'])[0]

            maker_username = initiated_by
            maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
            maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
            maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
            logging.info(f"####maker_checker_ids is {maker_checker_ids}")
            maker_name = maker_checker_ids[0]
            logging.info(f"maker_name is {maker_name}")


            #Adding data to user operations table
            
            data_to_insert = {
                'ID':str(user_id),
                'USERNAME': row_data['username'],
                'EMPLOYEE_NAME': row_data['employee_name'],
                'BRANCH_CODE': row_data['branch_code'],
                'BRANCH_NAME': row_data['branch_name'],
                'ROLE': row_data['role'],
                'ACTIVITY': 'Create',
                'STATUS': 'Create',
                'MAKER_ID': initiated_by,
                'CREATED_USER': maker_name,
                'USER_MODIFIED_DATE': currentTS,
                'LAST_UPDATED_DATE': currentTS
            }

            filtered_data = {k: v for k, v in data_to_insert.items() if v != ''}
            columns_list = ', '.join(filtered_data.keys())
            #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
            values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())

            insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
            try:
                update_data = group_access_db.execute_(insert_query)
                logging.info(f'update_data result----> {update_data}')
            except Exception as e:
                logging.error(f'Error executing insert query: {e}')
                
            try:
                query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
                active_directory_df = group_access_db.execute_(query)
                user_id = list(active_directory_df['id'])[0]
            except:
                traceback.print_exc()
                session.rollback()
                logging.warning('rolling back')
                session.close()
                logging.warning('closing session')
                return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
            
            if attributes:
                try:
                    query1 = f"SELECT * FROM `organisation_attributes`"
                    organisation_attributes_df = group_access_db.execute_(query1)
                except:
                    traceback.print_exc()
                    session.rollback()
                    logging.warning('rolling back')
                    session.close()
                    logging.warning('closing session')
                    return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
            else:
                traceback.print_exc()
                session.rollback()
                logging.warning('rolling back')
                session.close()
                logging.warning('closing session')
                return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}       
            
            result = generate_insert_user_org_mapping(user_id, orig_row_data, group_access_db) 
            to_insert = result['data'] if result['flag'] else []
            
            logging.info(f"#################333 TO_INSERT: {to_insert}")

            if to_insert:
                try:    
                    insert_query = generate_multiple_insert_query(to_insert, 'user_organisation_mapping')
                    logging.info(f" ## USER ORG insert query got is {insert_query}")
                    result = session.execute(insert_query)
                    
                    if not result:
                        session.rollback()
                        logging.warning('rolling back')
                        session.close()
                        logging.warning('closing session')
                        message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping"
                        
                        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}   

                    session.commit()
                    logging.warning('committing session')
                    session.close()
                    logging.warning('session closed')
                    message = f"User ID created and record sent for verification"
                    return {"flag": True,"errorCode":0, "message" : message,"isacTicketNo":isac_token}
                except Exception as e:
                    traceback.print_exc()
                    session.rollback()
                    logging.warning('rolling back')
                    session.close()
                    logging.warning('closing session')
                    return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
            
            else:
                session.rollback()
                logging.warning('rolling back')
                session.close()
                logging.warning('closing session')
                message = f"No data found for user {user_name} | user_id {user_id} to insert in user_organisation_mapping"
                return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
        else:
            logging.info("User is already exist in the acive directory so need to update the details")
            create_user_query = generate_update_query(row_data, 'active_directory',group_access_db)

            row_data['username'] = user_name

            
            maker_username = initiated_by
            maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
            maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
            maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
            logging.info(f"####maker_checker_employee_names is {maker_checker_ids}")
            maker_name = maker_checker_ids[0]
            logging.info(f"maker_name is {maker_name}")

            #Adding making uam maker details to row data for adding to active_directory_modifications
            row_data['maker_id'] = initiated_by
            maker_name = escape_apostrophe(maker_name)
            row_data['maker_name'] = maker_name
            row_data['maker_date'] = currentTS   
            row_data['last_updated'] = currentTS         

            create_user_query_2 = generate_insert_query(row_data, 'active_directory_modifications')

            
            result_1 = group_access_db.execute_(create_user_query)
            logging.info(f'create_user_query----{create_user_query} and result_1 is {result_1}')
            
            result_2 = group_access_db.execute_(create_user_query_2)
            logging.info(f'create_user_query_2----{create_user_query_2} and result_2 is {result_2}')

            query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
            active_directory_df = group_access_db.execute_(query)
            user_id = list(active_directory_df['id'])[0]


            maker_username = initiated_by
            maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
            maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
            maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
            logging.info(f"####maker_checker_ids is {maker_checker_ids}")
            maker_name = maker_checker_ids[0]
            logging.info(f"maker_name is {maker_name}")


            #Adding data to user operations table
            
            data_to_insert = {
                'ID':str(user_id),
                'USERNAME': user_name,
                'EMPLOYEE_NAME': row_data['employee_name'],
                'BRANCH_CODE': row_data['branch_code'],
                'BRANCH_NAME': row_data['branch_name'],
                'ROLE': row_data['role'],
                'ACTIVITY': 'Create',
                'STATUS': 'Create',
                'MAKER_ID': initiated_by,
                'CREATED_USER': maker_name,
                'USER_MODIFIED_DATE': currentTS,
                'LAST_UPDATED_DATE': currentTS
            }

            filtered_data = {k: v for k, v in data_to_insert.items() if v != ''}
            columns_list = ', '.join(filtered_data.keys())
            #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
            values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())

            insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
            try:
                update_data = group_access_db.execute_(insert_query)
                logging.info(f'update_data result----> {update_data}')
            except Exception as e:
                logging.error(f'Error executing insert query: {e}')





            session.commit()
            logging.warning('committing session')
            session.close()
            logging.warning('session closed')
            message = f"User ID created and record sent for verification"
            return {"flag": True,"errorCode":0, "message" : message,"isacTicketNo":isac_token}



        
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    

def generate_update_query(dict_data, table_name, group_access_db, db = "mysql"):
    columns_list,values_list = [],[]
    logging.debug(f"dict_data: {dict_data}")


    try:
        if table_name=='active_directory':
            del dict_data['route_name']
            del dict_data['session_id']
            del dict_data['tenant_id']
            del dict_data['sources']
            del dict_data['changed_fields']
            del dict_data['initiatedBy']
            del dict_data['approvedBy']
    except:
        pass

    try:
        if table_name=='active_directory_modifications':
            del dict_data['id']
    except:
        pass

    username = dict_data.pop('username')
    id_ = dict_data.pop('id')

    dict_data = {k.lower(): v for k, v in dict_data.items()}
    logging.info(f"####dict_data is {dict_data}")

    query_ = f"SELECT COLUMN_NAME FROM all_tab_columns WHERE table_name = 'ACTIVE_DIRECTORY'"
    query_result = group_access_db.execute_(query_)
    column_names = list(query_result['COLUMN_NAME'])
    logging.info(f'###column_names {column_names}')



    all_columns = [elem.lower() for elem in column_names]
    all_columns.remove('id')
    all_columns.remove('username')
    all_columns.remove('login_attempts')
    all_columns.remove('created_date')
    all_columns.remove('last_updated')
    all_columns.remove('previous_status')
    all_columns.remove('deleted_date')
    all_columns.remove('user_audit')

    set_clause_list = []

    for column, value in dict_data.items():
        if isinstance(value, dict):
            value = json.dumps(value)
        set_clause_list.append(f"{column} = '{value}'")

    for column in all_columns:
        if column not in dict_data:
            set_clause_list.append(f"{column} = ''")

    set_clause = ', '.join(set_clause_list)

    update_query = f"UPDATE {table_name} SET {set_clause} WHERE username = '{username}'"
    logging.info(f"####update_query is {update_query}")
    return update_query



def approved_user(row_data, group_access_db, queue_db, user,isac_token):
    # TRY USING COMMIT AND ROLLBACK WITH SQLALCHEMY
    orig_row_data = row_data.copy()
    logging.info(f"####row da in the approved_user is {row_data}")
    try:
        ad_user_status = row_data.get('status',None)
        attributes = row_data.pop('attributes', [])
        maker_id = row_data.get('maker_id',None)
        initiated_by = row_data.get('initiatedBy',None)
        approved_by=row_data.get('approvedBy',None)
        user_name = row_data['username']
        logging.info(f"user_name got : {user_name}")
    except:
        traceback.print_exc()
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    
    engine = group_access_db.engine
    Session = sessionmaker(bind = engine)
    session = Session()

    user_modified_date_ = row_data['user_modified_date']
    user_modified_date_ = datetime.strptime(user_modified_date_, "%a, %d %b %Y %H:%M:%S %Z")
    user_modified_date_ = user_modified_date_.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
    logging.info(f"###user_modified_date_ is {user_modified_date_}")
    
    try:

        #Handling the apostrophy
        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value
        
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

        get_user_query = f"""SELECT ADM.user_email,ADM.role,ADM.username,ADM.password,
                TO_CHAR(ADM.created_date, 'DD-MON-YY HH.MI.SS.FF6 AM') created_date,
                TO_CHAR(ADM.last_updated, 'DD-MON-YY HH.MI.SS.FF6 AM') last_updated,
                ADM.selectedtheme,ADM.employee_code,ADM.employee_id,ADM.employee_name,ADM.branch_code,
                ADM.branch_name,ADM.department_code,ADM.department_name,ADM.role_code,ADM.role_name,ADM.mobile_no,ADM.address,
                ADM.supervisor_code,ADM.status,ADM.login_attempts,ADM.previous_status,ADM.old_employee_name,ADM.old_role_name,ADM.old_branch_name,ADM.created_user,
                TO_CHAR(ADM.DELETED_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') deleted_date,
                TO_CHAR(ADM.USER_MODIFIED_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') user_modified_date,
                TO_CHAR(ADM.USER_DISABLED_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') user_disabled_date,
                ADM.isac_ticket_no, ADM.isac_ticket_status, ADM.maker_id, ADM.maker_name,
                TO_CHAR(ADM.MAKER_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') maker_date
            FROM 
                hdfc_group_access.active_directory_modifications ADM
            WHERE USERNAME = '{user_name}' AND STATUS not in ('approved','rejected')"""


        logging.info(f"###get_user_query is {get_user_query}")
        user_details_df = group_access_db.execute_(get_user_query)
        logging.info(f'update_user_data result----> {user_details_df}')

        if user_details_df.empty:
            logging.info('No data fetched from the database')
        else:
            # Convert the DataFrame to a dictionary
            user_data_dict_data = user_details_df.to_dict(orient='records')[0]
            user_data_dict = {}

            for key, value in user_data_dict_data.items():
                lower_key = key.lower()
                if lower_key not in user_data_dict:
                    user_data_dict[lower_key] = value

            logging.info(f'update_user_data result----> {user_data_dict}')
        
        # Removing unwanted fields
        user_data_dict.pop('created_date',None)
        user_data_dict.pop('login_attempts',None)
        #adding new dates 
        user_data_dict['last_updated'] = currentTS

        #Adding uam checker details into the active directory
        checker_username = approved_by
        checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{checker_username}'"
        checker_ids_query = group_access_db.execute_(checker_ids_query)
        checker_ids = list(checker_ids_query['EMPLOYEE_NAME'])
        logging.info(f"####maker_checker_ids is {checker_ids}")
        checker_name = checker_ids[0]
        logging.info(f"maker_name is {checker_name}")

        user_data_dict['checker_id'] = approved_by
        user_data_dict['checker_name'] = checker_name
        user_data_dict['checker_date'] = currentTS
        user_data_dict['login_attempts'] = 3

        checker_name=escape_apostrophe(checker_name)

        #Based on the status dates changing
        if user_data_dict['status'] == 'revoke':
            user_data_dict['status'] = 'enable'
        if user_data_dict['status'] == 'unlock':
            user_data_dict['status'] = 'enable'
        if user_data_dict['status'] == 'waiting':
            user_data_dict['status'] = 'enable'
            user_data_dict['created_date'] = currentTS
        if user_data_dict['status'] == 'disable':
            user_data_dict['user_disabled_date'] = currentTS
        if user_data_dict['status'] == 'delete':
            user_data_dict['deleted_date'] = currentTS

        logging.info(f'user_data_dict after modifications1 ----> {user_data_dict}')

        # Remove entries with None values
        user_data_dict = {k: v for k, v in user_data_dict.items() if v is not None}
        logging.info(f'user_data_dict after modifications2 ----> {user_data_dict}')

        #set_clause = ", ".join([f"{k} = '{v}'" for k, v in user_data_dict.items()])
        set_clause = ", ".join([f"{k} = '{str(v).replace("'", "''")}'" for k, v in user_data_dict.items()])
        logging.info(f'set_clause is ----> {set_clause}')
        update_query = f"UPDATE active_directory SET {set_clause} WHERE USERNAME = '{user_name}'"

        try:
            # Execute the update query
            update_query_ad = group_access_db.execute_(update_query)
            logging.info(f'Executed update active directory query: {update_query_ad}')
            
            try:
                approved_query = f"UPDATE active_directory_modifications SET STATUS = 'approved' where USERNAME = '{user_name}' AND STATUS not in ('approved','rejected')"
                update_query_adm = group_access_db.execute_(approved_query)
                logging.info(f'Executed update active directory modifications query: {update_query_adm}')
            
            except Exception as e:
                logging.info(f"Active directory modifications user data not updated error is {e}")
            
            query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
            active_directory_df = group_access_db.execute_(query)
            user_data = active_directory_df.to_dict(orient='records')[0]
            logging.info(f"####user_data is {user_data}")

            maker_username = maker_id
            maker_checker_ids_query = f"SELECT USERNAME, EMPLOYEE_NAME FROM `active_directory` WHERE `username` in ('{maker_username}','{approved_by}')"
            maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
            username_to_employee_name = dict(zip(maker_checker_ids_query['USERNAME'], maker_checker_ids_query['EMPLOYEE_NAME']))
            logging.info(f"####maker_checker_ids is {username_to_employee_name}")
            maker_name = username_to_employee_name.get(maker_username, 'Unknown Maker')
            checker_name = username_to_employee_name.get(approved_by, 'Unknown Checker')
            logging.info(f"maker_id is {maker_name} and checker_id is {checker_name}")

            if ad_user_status == 'waiting':
                ad_user_status = 'enable'

            #Adding data to user operations table
            data_to_insert = {
                'ID': str(user_data['id']),
                'USERNAME': user_name,
                'EMPLOYEE_NAME': user_data['employee_name'],
                'BRANCH_CODE': user_data['branch_code'],
                'BRANCH_NAME': user_data['branch_name'],
                'ROLE': user_data['role'],
                'ACTIVITY': 'approved',
                'STATUS' : ad_user_status,
                'MAKER_ID': maker_id,
                'CREATED_USER': maker_name,
                'USER_MODIFIED_DATE': user_modified_date_,
                'CHECKER_ID': approved_by,
                'CHECKER_NAME': checker_name,
                'CHECKER_DATE': currentTS,
                'LAST_UPDATED_DATE': currentTS
            }

            filtered_data = {k: v for k, v in data_to_insert.items() if v not in ('', None)}
            columns_list = ', '.join(filtered_data.keys())
            #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
            values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())

            insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
            try:
                update_data = group_access_db.execute_(insert_query)
                logging.info(f'update_data result----> {update_data}')
            except Exception as e:
                logging.error(f'Error executing insert query: {e}')


        except Exception as e:
            logging.info(f"Active directory user data not updated error is {e}")

        
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    
    session.commit()
    session.close()
 
    return {"flag": True,"errorCode":0, "message" : 'User details approved',"isacTicketNo":isac_token}



def rejected_user(row_data, group_access_db, queue_db, user,isac_token, approved_by):
    # TRY USING COMMIT AND ROLLBACK WITH SQLALCHEMY
    orig_row_data = row_data.copy()
    try:
        logging.info(f"###row_data is {row_data}")
        approved_by = approved_by
        maker_id = row_data.get('maker_id',None)
        initiated_by = row_data.get('initiatedBy',None)
        attributes = row_data.pop('attributes', [])
        user_name = row_data['username']
        logging.info(f"user_name got : {user_name}")
        rejected_comments = row_data.get('rejected_comments','')
    except:
        traceback.print_exc()
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    
    user_modified_date_ = row_data['user_modified_date']
    user_modified_date_ = datetime.strptime(user_modified_date_, "%a, %d %b %Y %H:%M:%S %Z")
    user_modified_date_ = user_modified_date_.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
    logging.info(f"###user_modified_date_ is {user_modified_date_}")
    
    engine = group_access_db.engine
    Session = sessionmaker(bind = engine)
    session = Session()

    current_ist = datetime.now(pytz.timezone(tmzone))
    currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
    
    try:

        #Handling the apostrophy -
        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value

        #Fetching the status of the user from the active directory
        get_user_query = f"SELECT status FROM active_directory WHERE USERNAME = '{user_name}' AND STATUS NOT IN ('approved','rejected')"
        get_status_df = group_access_db.execute_(get_user_query)

        if get_status_df.empty:
            logging.info('No data fetched from the database')
            return {"flag": False,"errorCode":0, "message" : 'No user found',"isacTicketNo":isac_token}
        else:
            user_status = get_status_df['status'].to_list()[0]
            logging.info(f"###user_status is {user_status}")

            checker_username = approved_by
            checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{checker_username}'"
            checker_ids_query = group_access_db.execute_(checker_ids_query)
            checker_ids = list(checker_ids_query['EMPLOYEE_NAME'])
            logging.info(f"####maker_checker_ids is {checker_ids}")
            checker_name = checker_ids[0]
            logging.info(f"maker_name is {checker_name}")
            checker_name = escape_apostrophe(checker_name)

            #ad means active_directory, adm means active_directory_modifications
            if user_status.lower() == 'waiting':
                ad_query = f"UPDATE active_directory SET STATUS = 'rejected', CHECKER_ID = '{approved_by}', CHECKER_NAME = '{checker_name}',CHECKER_DATE = '{currentTS}'  WHERE USERNAME = '{user_name}'"
                ad_query_result = group_access_db.execute_(ad_query)
                logging.info(f"###ad_query_result is {ad_query_result}")

            ad_modiication_query = f"UPDATE active_directory_modifications SET STATUS = 'rejected' WHERE USERNAME = '{user_name}' AND STATUS NOT IN ('approved','rejected')"
            ad_modiication_query_result = group_access_db.execute_(ad_modiication_query)
            logging.info(f"###ad_modiication_query_result is {ad_modiication_query_result}")



            query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
            active_directory_df = group_access_db.execute_(query)
            user_data = active_directory_df.to_dict(orient='records')[0]
            logging.info(f"####user_data is {user_data}")

            maker_username = maker_id
            maker_checker_ids_query = f"SELECT USERNAME, EMPLOYEE_NAME FROM `active_directory` WHERE `username` in ('{maker_username}','{approved_by}')"
            maker_checker_ids_query = group_access_db.execute_(maker_checker_ids_query)
            username_to_employee_name = dict(zip(maker_checker_ids_query['USERNAME'], maker_checker_ids_query['EMPLOYEE_NAME']))
            logging.info(f"####maker_checker_ids is {username_to_employee_name}")
            maker_name = username_to_employee_name.get(maker_username, 'Unknown Maker')
            checker_name = username_to_employee_name.get(approved_by, 'Unknown Checker')
            logging.info(f"maker_id is {maker_name} and checker_id is {checker_name}")

            if user_status.lower() == 'waiting':
                ad_user_status = 'rejected'
            else:
                ad_user_status = user_data['status']
            #Adding data to user operations table
            data_to_insert = {
                'ID': str(user_data['id']),
                'USERNAME': user_name,
                'EMPLOYEE_NAME': user_data['employee_name'],
                'BRANCH_CODE': user_data['branch_code'],
                'BRANCH_NAME': user_data['branch_name'],
                'ROLE': user_data['role'],
                'ACTIVITY': 'rejected',
                'STATUS' : ad_user_status,
                'MAKER_ID': maker_id,
                'CREATED_USER': maker_name,
                'USER_MODIFIED_DATE': user_modified_date_,
                'CHECKER_ID': user,
                'CHECKER_NAME': checker_name,
                'CHECKER_DATE': currentTS,
                'LAST_UPDATED_DATE': currentTS,
                'REJECTED_COMMENTS': rejected_comments
            }

            filtered_data = {k: v for k, v in data_to_insert.items() if v not in ('', None)}
            columns_list = ', '.join(filtered_data.keys())
            #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
            values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())

            insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
            try:
                update_data = group_access_db.execute_(insert_query)
                logging.info(f'update_data result----> {update_data}')
            except Exception as e:
                logging.error(f'Error executing insert query: {e}')
        
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        return {"flag": False,"errorCode":10, "message" : 'Invalid user details',"isacTicketNo":isac_token}
    
    session.commit()
    session.close()
 
    return {"flag": True,"errorCode":0, "message" : 'User details rejected',"isacTicketNo":isac_token}








def generate_insert_user_org_mapping(user_id, rowdata, group_access_db):
    try:
        to_insert = []
        attributes_list = rowdata['attributes']
        sequence_id = 1
        for attributes in attributes_list:
            username = rowdata["username"]

            group_access_db.execute_(f'delete from user_organisation_mapping where user_id = {user_id}')
    
            attribute_ids = {}
            for attribute in attributes:
                attribute = attributes[attribute]
                attribute_ids[attribute] = group_access_db.execute_(f"select attribute_id from attribute_dropdown_definition where value = '{attribute}'")["attribute_id"].to_list()[0]
            
            for attribute_id in attribute_ids:
                to_insert.append({
                    "user_id": user_id,
                    "sequence_id": str(sequence_id),
                    "type": "user",
                    "organisation_attribute": attribute_ids[attribute_id],
                    "value": attribute_id
                })
            sequence_id += 1
        
        return {"flag": True, "data" : to_insert}
    except:
        traceback.print_exc()
        message = f"Something went wrong while generating rows to be inserted."
        return {"flag": False, "message" : message}
    

def generate_multiple_insert_query(data, table_name):
    # Insert query is generated for user_organisation_mapping
    values_list = []
    for row in data:
        values_list_element = []
        for column, value in row.items():
            if column != 'id':
                if value is None:
                    values_list_element.append(f"NULL")
                else:
                    values_list_element.append(f"'{value}'")
        values_list.append('(' + ', '.join(values_list_element) + ')')
    columns = list(data[0].keys())
    if 'id' in columns: columns.remove('id')

    values_list = ', '.join(values_list)
    columns_list = ', '.join([f"{x}" for x in columns])
    query = f"INSERT INTO {table_name} ({columns_list}) VALUES {values_list}"
    logging.debug(f"Multiple insert query for {table_name}: {query}")
    
    return query


@app.route("/modify_user", methods=['POST', 'GET'])
def modify_user():
    data = request.json
    logging.info(f'Request data in modify_user: {data}')
    req_data_cpy=data.copy()
    tenant_id = data.get('tenant_id',None)
    session_id = data.get('session_id', None)
    user = data.get('user',None)
    sources=data.get('sources',{"role": "user"})
    tenant_id=data.get('tenant_id',os.environ['TENANT_ID'])
    route_name=data.get('route_name',None)
    user=data.get('initiatedBy',None)
    session_id=data.get('session_id',None)
    approved_by=data.get('approvedBy',None)
    initiated_by = data.get('initiatedBy',None)
    isac_token=data.get("isacTicketNo",None)
    changed_data = data.get('changed_fields',None)
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    try:
        operation = data.pop('operation').lower()
        logging.info(f"####operation is {operation}")
        user_name = data.get('username', None)

        #Code for handling the apostrophy in the employee_name and user_email
        employee_name_changed = None
        user_email_changed = None

        employee_name=data.get('employee_name', None)
        user_email=data.get('user_email', None)
        if 'changed_fields' in data:
            employee_name_changed=data['changed_fields'].get('employee_name', None)
            user_email_changed=data['changed_fields'].get('user_email', None)
        user_id=data.get("id",None)

        if user_name:
            user_name = user_name.lower()
            data['username'] = data['username'].lower()

        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value
        
        if employee_name:
            employee_name=escape_apostrophe(employee_name)
            data['employee_name'] = employee_name
        if user_email:
            user_email=escape_apostrophe(user_email)
            data['user_email'] = user_email
        if employee_name_changed:
            employee_name_changed=escape_apostrophe(employee_name_changed)
            data['changed_fields']['employee_name'] = employee_name_changed
        if user_email_changed:
            user_email_changed=escape_apostrophe(user_email_changed)
            data['changed_fields']['user_email'] = user_email_changed
        
        print(f" ### employee GOT IS {employee_name}")
    except:
        message = "Received unexpected request data."
        result={"flag": False, "message" : message}
    
    trace_id = generate_random_64bit_string()
    attr = ZipkinAttrs(
            trace_id=trace_id,
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='user_management',
            zipkin_attrs=attr,
            span_name='modify_user',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        queue_db = DB('queues', **db_config)

        if operation == 'edit':
            user_name=data.get('username',None)
            get_id=f"select id from active_directory where username='{user_name}'"
            user_id=group_access_db.execute_(get_id)['id'].to_list()[0]
            result = edit_user(user_id,data, group_access_db, user,isac_token, changed_data, approved_by, initiated_by)
            
        elif operation == 'create':
            result = create_user(data, group_access_db, queue_db, user,isac_token)
        elif operation == 'approved':
            result = approved_user(data, group_access_db, queue_db, user,isac_token)
        elif operation == 'rejected':
            result = rejected_user(data, group_access_db, queue_db, user,isac_token, approved_by)
        
        else:
            result = {'message':'Didnot receive proper operator'}
        
        headers = {'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
        requests.post('https://queueapi:443/clear_cache', headers=headers,verify=False)

        return_data = result
            
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

        logging.info(f" #### um info return data got is {return_data}")
        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user_name,
                        "api_service": "modify_user", "service_container": "user_management", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": str(return_data['message']), "trace_id": trace_id, "session_id": session_id,"status":str(return_data['flag'])}
        insert_into_audit(audit_data)

        return jsonify(result)

def get_attributes_for_active_directory(active_directory_dict, group_access_db):
    for active_directory in active_directory_dict:
        id  = active_directory['id']

        user_org_dict = group_access_db.execute_(f'select * from `user_organisation_mapping` where user_id= {id}').to_dict(orient = "records")
        max_sequence = group_access_db.execute_(f'SELECT max(sequence_id) as "max(sequence_id)" FROM `user_organisation_mapping` where user_id= {id}')['max(sequence_id)']
        
        logging.info(f"############# USER ORG DICT: {user_org_dict}")
        logging.info(f"################ MAX SEQ: {max_sequence}")
        try:
            max_sequence = max_sequence[0]
        except:
            max_sequence = 0

        #waiting converted to enable
        if active_directory['status'] == 'waiting':
            active_directory['status'] = 'enable'
        
        active_directory['attributes'] = []
        try:
            if max_sequence:
                for i in range(max_sequence):
                    active_directory['attributes'].append({})
                for user_org in user_org_dict:      
                    parent_attribute = list(group_access_db.execute_(f'select attribute from `organisation_attributes` where att_id = {user_org["organisation_attribute"]}')['attribute'])[0]
                    active_directory['attributes'][user_org['sequence_id']-1][parent_attribute] = user_org['value']
                
                active_directory['attributes'] = [i for i in active_directory['attributes'] if i!={}]
        except:
            logging.info("Error collecting attributes")
    
    return active_directory_dict

def fetch_group_attributes_json(tenant_id, group_access_db=""):
    if group_access_db =="":
        group_access_db = DB('group_access', **db_config)
    hgroups_query = f"SELECT h_group, id, h_order FROM organisation_hierarchy"
    hierarchy_table = group_access_db.execute_(hgroups_query).to_dict(orient = "records")
    dropdown_query = f"SELECT attribute_id, parent_attribute_value, value FROM attribute_dropdown_definition"
    dropdown_table  = group_access_db.execute_(dropdown_query).to_dict(orient = "records")
    hgs = group_access_db.execute_(hgroups_query )["h_group"].to_list()
    horders = group_access_db.execute_(hgroups_query )["h_order"].to_list()
    ids = group_access_db.execute_(hgroups_query )["id"].to_list()
    hg_ho = dict(zip(hgs, horders))

    data = {}
    if 'group_attributes' not in data:
        data['group_attributes'] = {}
    if 'hierarchy' not in data:
        data['hierarchy'] = {}
    try:
        sources, hierarchy = get_sources_and_hierarchy(group_access_db, False)
        data['sources'] = sources
        data['hierarchy'] = hierarchy
    except:
        traceback.print_exc()
        message = "Could not load source and/or hierarchy for group_attributes. Check logs."
        return jsonify({"flag": False, "message" : message})   
    
    dropdown_ids =  group_access_db.execute_(dropdown_query)["attribute_id"].to_list()
    for hierarchy_row in hierarchy_table:
    
        
        if hierarchy_row["id"] in dropdown_ids:
            value = [x["value"] for x in dropdown_table if x["attribute_id"] == hierarchy_row["id"] and x["value"] != ""]
            parent_value = [x["parent_attribute_value"] for x in dropdown_table if x["attribute_id"] == hierarchy_row["id"]]
            if len(parent_value)>=0:
                if parent_value[0] is None:
                    if hierarchy_row['h_group'] not in data['group_attributes']:
                        data['group_attributes'][hierarchy_row['h_group']] = {}
                    if hierarchy_row['h_order'] not in data['group_attributes'][hierarchy_row['h_group']]:
                        data['group_attributes'][hierarchy_row['h_group']][hierarchy_row['h_order']] = []
                    data['group_attributes'][hierarchy_row['h_group']][hierarchy_row['h_order']].extend(value)
                else:
                    if hierarchy_row['h_group'] not in data['group_attributes']:
                        data['group_attributes'][hierarchy_row['h_group']] = {}
                    if hierarchy_row['h_order'] not in data['group_attributes'][hierarchy_row['h_group']]:
                        data['group_attributes'][hierarchy_row['h_group']][hierarchy_row['h_order']] = {}
                    if parent_value[0] not in data['group_attributes'][hierarchy_row['h_group']][hierarchy_row['h_order']]:
                        data['group_attributes'][hierarchy_row['h_group']][hierarchy_row['h_order']][parent_value[0]] = []
                    data['group_attributes'][hierarchy_row['h_group']][hierarchy_row['h_order']][parent_value[0]].extend(value)
    
    # try:
    non_user_dropdown = get_non_user_dropdown(tenant_id, group_access_db)
    data['non_user_dropdown'] = non_user_dropdown
    return data


@app.route("/audit_uam", methods=['POST', 'GET'])
def audit_uam():
    data = request.json
    username = data.get('username','')
    tenant_id = data.get('tenant_id', '')
    db_config['tenant_id'] = tenant_id
    group_access_db = DB('group_access', **db_config)
    query = f"select user_audit from active_directory where username='{username}'"
    res = group_access_db.execute_(query)
    res = res['user_audit'][0]
    res = json.loads(res)
    return jsonify({'flag': True, 'modified_fields': res})


    

@app.route("/show_existing_users", methods=['POST', 'GET'])
def show_existing_users():
    headers=request.headers
    headers_dict={}

    headers=request.headers
    for k,v in headers.items():
        logging.info(f"## Header key Val is {k}:{v}")
        headers_dict[k]=v
    logging.info(f"## Headers got are {headers}")
    bearer_token = headers_dict.get('Authorization',None)
    secret_key=headers_dict.get('apiKey',None)
    
    logging.info(f"### Bearer got is {bearer_token}")
    if bearer_token:
        token=bearer_token.split(" ")[2]
        logging.info(f" ## Token got is {token}")
        token_response=decode_generated_token(token,secret_key)
        logging.info(f"## token response got is {token_response}")
    else:
        pass
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    session_id = data.get('session_id', None)

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
    attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='user_management',
            zipkin_attrs=attr,
            span_name='show_exisiting_users',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        
        db_config['tenant_id'] = tenant_id
        flag = data.pop('flag', None)
        user = data.get('user','')
        logging.info(f'user is: {user}')
        group_access_db = DB('group_access', **db_config)
        try:
            active_directory_user_query = f"SELECT ROLE FROM `active_directory` where username = '{user}'"
            active_directory_user_df = group_access_db.execute_(active_directory_user_query)
            user_role = active_directory_user_df.iloc[0]['ROLE']
        except:
            user_role = ''
        logging.info(f"###user_role is {user_role}")
        
        
        if flag == 'search':
            try:
                text = data['data'].pop('search_word')
                table_name = data['data'].pop('table_name', 'active_directory')
                start_point = data['data']['start'] - 1
                end_point = data['data']['end']
                header_name = data['data'].get('column', None)
                offset = end_point - start_point
            except:
                traceback.print_exc()
                message = f"Input data is missing "
                response_data={"flag": False, "message" : message}    
            
            table_name = 'active_directory'
            columns_list = list(group_access_db.execute_(f"SHOW COLUMNS FROM `{table_name}`")['Field'])
       
            files, total = master_search(tenant_id = tenant_id, text = text, table_name = table_name, start_point = 0, offset = 10, columns_list = columns_list, header_name=header_name)
            
            active_directory_dict = get_attributes_for_active_directory(files, group_access_db)
            logging.info(f'active_directory_dict---------{active_directory_dict}')

            if end_point > total:
                end_point = total
            if start_point == 1:
                pass
            else:
                start_point += 1
            
            pagination = {"start": start_point, "end": end_point, "total": total}
            
            response_data = {"flag": True, "data": files, "pagination":pagination}
        else:

            try:
                

                #changes for UAM Maker and UAM Checker
                if user_role == 'UAM Checker':
                    active_directory_query = f"SELECT AD.id, ADM.* FROM `active_directory_modifications` ADM INNER JOIN `active_directory` AD ON ADM.username = AD.username WHERE ADM.STATUS NOT IN ('approved','rejected') order by ADM.LAST_UPDATED DESC"
                    active_directory_df = group_access_db.execute_(active_directory_query)
                elif user_role == 'UAM Reviewer':
                    active_directory_query = f"SELECT * FROM `active_directory` WHERE STATUS NOT IN ('rejected','closed','waiting') order by CREATED_DATE DESC"
                    active_directory_df = group_access_db.execute_(active_directory_query)

                    active_directory_query_ = f"SELECT AD.id, ADM.* FROM `active_directory_modifications` ADM INNER JOIN `active_directory` AD ON ADM.username = AD.username WHERE ADM.STATUS NOT IN ('approved','rejected')"
                    active_directory_df_ = group_access_db.execute_(active_directory_query_)
                    usernames_set = set(active_directory_df_['username'])
                    active_directory_df['pending'] = active_directory_df['username'].isin(usernames_set)
                    

                else:
                    active_directory_query_ = f"SELECT AD.id, ADM.* FROM `active_directory_modifications` ADM INNER JOIN `active_directory` AD ON ADM.username = AD.username WHERE ADM.STATUS NOT IN ('approved','rejected')"
                    active_directory_df_ = group_access_db.execute_(active_directory_query_)


                    active_directory_query = f"SELECT * FROM `active_directory` WHERE STATUS NOT IN ('rejected','closed','waiting') order by CREATED_DATE DESC"
                    active_directory_df = group_access_db.execute_(active_directory_query)

                    usernames_set = set(active_directory_df_['username'])
                    active_directory_df['pending'] = active_directory_df['username'].isin(usernames_set)




                
                logging.info(f'active_directory_df---------{active_directory_df}')


            except:
                traceback.print_exc()
                message = "Could not load from Active Directory"
                response_data = {"flag": False, "message" : message}
            
            active_user_ids = ','.join([str(x) for x in list(active_directory_df['id'])])
            
            try:
                if active_user_ids:
                    organisation_mapping_query = f"SELECT `user_organisation_mapping`.*, `organisation_attributes`.`attribute`, `organisation_attributes`.`source` FROM `user_organisation_mapping`, `organisation_attributes` WHERE `user_id` in ({active_user_ids}) AND `organisation_attributes`.`id` = `user_organisation_mapping`.`organisation_attribute`"
                    organisation_mapping_df = group_access_db.execute_(organisation_mapping_query)
                else:
                    organisation_mapping_query = f"SELECT `user_organisation_mapping`.*, `organisation_attributes`.`attribute`, `organisation_attributes`.`source` FROM `user_organisation_mapping`, `organisation_attributes` WHERE `user_id` in ('abcde') AND `organisation_attributes`.`id` = `user_organisation_mapping`.`organisation_attribute`"
                    organisation_mapping_df = group_access_db.execute_(organisation_mapping_query)
                if isinstance(organisation_mapping_df, pd.DataFrame):
                    if organisation_mapping_df.empty:
                        message = "No users present in User Organisation Mapping"
                        logging.exception(message)
            except:
                traceback.print_exc()
                message = "Could not load from User Organisation Mapping"
                response_data={"flag": False, "message" : message}
            
            hgroups = "SELECT h_group, id FROM `organisation_hierarchy`"
            exec_ = group_access_db.execute_(hgroups)
            hgs = exec_['h_group'].tolist()
            ids = exec_['id'].tolist()

            hgroups = {}
            for i in range(len(hgs)):
                if hgs[i] not in hgroups:
                    hgroups[hgs[i]] = [ids[i]]
                else:
                    hgroups[hgs[i]].append(ids[i])
            
            leaf_nodes=[]
            result = fetch_group_attributes_json(tenant_id, group_access_db = group_access_db)
            logging.info(f"######### Show users dropdown result: {result}")
            
            dropdown_definition = {}
            grp_attributes = result["group_attributes"]
            for grp_attribute in grp_attributes:
                grp_attribute = grp_attributes[grp_attribute]
                for attribute in grp_attribute:
                    dropdown_definition[attribute] = grp_attribute[attribute]

            active_directory_dict = active_directory_df.to_dict(orient= 'records')
            active_directory_dict = [{key: value for key, value in entry.items() if key != 'FIRST_NAME' and key != 'LAST_NAME' and key != 'EMPLOYEE_ID'} for entry in active_directory_dict]
            active_directory_dict = get_attributes_for_active_directory(active_directory_dict, group_access_db)
            
            columns_to_remove = ['attributes', 'password', 'status', 'id','selectedtheme','login_attempts', 'previous_status','LOGIN_ATTEMPTS', 'PREVIOUS_STATUS']
            columns_to_display = list(active_directory_df.columns) + ['attributes']
            for element in columns_to_remove:
                columns_to_display.remove(element)
                
            
            try:
                field_definition_query = f"SELECT * FROM `field_definition` WHERE `status` = 1 and id not in (1,2,7,13,14)"
                field_definition_df = group_access_db.execute_(field_definition_query)
                
                field_definition_df_static = field_definition_df[~field_definition_df['type'].isin(['dropdown','checkbox'])]
                field_definition_df_dynamic = field_definition_df[field_definition_df['type'].isin(['dropdown','checkbox'])]
                  
                field_definition_dict_static = field_definition_df_static.to_dict(orient= 'records')  
                field_definition_dict_dynamic = field_definition_df_dynamic.to_dict(orient= 'records')
                
                for idx, row in enumerate(field_definition_dict_dynamic):
                    if row['unique_name'] in leaf_nodes:
                        field_definition_dict_dynamic[idx]['multiple'] = True
                    else:
                        field_definition_dict_dynamic[idx]['multiple'] = False
            except:
                traceback.print_exc()
                message = "Could not load from Active Directory"
                response_data={"flag": False, "message" : message}

            #User Id and employee code need to be in upper case while entering
            for entry in field_definition_dict_static:
                if entry["id"] == 9 or entry["id"] == 5:
                    entry["isCapital"] = True
            
            logging.info(f"###field_definition_dict_static df id : {field_definition_dict_static}")
            logging.info(f"################## field_definition_df: {field_definition_df} ")
            logging.info(f"################## COlumns to display: {columns_to_display}")
            
            headers_list = []
            for header in columns_to_display:
                try:
                    display_name = list(field_definition_df[field_definition_df['unique_name'] == header].display_name)[0]
                    headers_list.append({'display_name': display_name, 'unique_name': header})
                except:
                    traceback.print_exc()
                    logging.error(f"Check configuration for {header}")
                    pass
            
            data = {
                "header" : headers_list,
                "rowdata" : active_directory_dict,
                "dropdown_definition": dropdown_definition,
                "field_def_static": field_definition_dict_static,
                "field_def_dynamic": field_definition_dict_dynamic,
                
            }

            pending_users = []
            pending_users_query = f"SELECT username FROM active_directory_modifications WHERE status NOT IN ('approved','rejected')"
            pending_users_df = group_access_db.execute_(pending_users_query)
            if pending_users_df.empty:
                pending_users = []
            else:
                pending_users = pending_users_df["username"].to_list()
        
            logging.info(f"####pending_users are {pending_users}")

            if user_role == 'UAM Maker':
                data['show_create_user'] = True
                data['show_edit_user'] = True
                data['show_delete_user'] = True
                data['show_activate_user'] = True
                data['show_unlock_user'] = True
                data['show_logout'] = True
                data['pendingApprovalUsersList'] = pending_users
            elif user_role in ('UAM Reviewer', 'UAM Checker'):
                data['show_create_user'] = False
                data['show_edit_user'] = True
                data['show_delete_user'] = False
                data['show_activate_user'] = False
                data['show_unlock_user'] = False
                data['show_logout'] = True
                data['show_info_user'] = True
            if user_role == 'UAM Checker':
                data['show_approval_user'] = True
                data['show_info_user'] = True
            if user_role == 'UAM Reviewer':
                data['show_reports'] = True
                data['pendingApprovalUsersList'] = pending_users
            
            response_data = {"flag": True, "data" : data}
        
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
        
        logging.info(f"##For show existing users Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(response_data)

def generate_insert_query(dict_data, table_name, db = "mysql"):
    columns_list,values_list = [],[]
    logging.debug(f"dict_data: {dict_data}")


    try:
        if table_name=='active_directory':
            del dict_data['route_name']
            del dict_data['session_id']
            del dict_data['tenant_id']
            del dict_data['sources']
            del dict_data['changed_fields']
            del dict_data['initiatedBy']
            del dict_data['approvedBy']
    except:
        pass

    try:
        if table_name=='active_directory_modifications':
            del dict_data['id']
    except:
        pass

    logging.info(f"####dict_data is {dict_data}")

    for column, value in dict_data.items():
        if type(value)==dict:
            value = json.dumps(value)
        else:
            value = value
        columns_list.append(f"{column}")
        values_list.append(f"'{value}'")

    columns_list = ', '.join(columns_list)
    values_list= ', '.join(values_list)
    logging.info(f'table_name------:{table_name}')
    logging.info(f'columns_list-----{columns_list}')

    insert_query = f"INSERT INTO {table_name} ({columns_list}) VALUES ({values_list})"
    return insert_query

def generate_insert_query_mssql(dict_data, table_name):
    columns_list,values_list = [],[]
    logging.debug(f"dict_data: {dict_data}")

    for column, value in dict_data.items():
        columns_list.append(f"{column}")
        values_list.append(f"'{value}'")

    columns_list = ''.join(columns_list)
    values_list= ', '.join(values_list)

    insert_query = f'INSERT INTO {table_name} ({columns_list}) VALUES ({values_list})'
    return insert_query

def master_search(tenant_id, text, table_name, start_point, offset, columns_list, header_name):
    elastic_input = {}
    
    elastic_input['columns'] = columns_list
    elastic_input['start_point'] = start_point
    elastic_input['size'] = offset
    if header_name:
        elastic_input['filter'] = [{'field': header_name, 'value': "*" + text + "*"}]
    else:
        elastic_input['text'] = text
    elastic_input['source'] = table_name
    elastic_input['tenant_id'] = tenant_id
    files, total = elasticsearch_search(elastic_input)
    return files, total

def select_star(table):
    return f"SELECT * FROM `{table}`"    

def get_sources_and_hierarchy(group_access_db,user_only=True):
    organisation_hierarchy_query = select_star('organisation_hierarchy')
    organisation_hierarchy_df = group_access_db.execute_(organisation_hierarchy_query)
    
    sources = {}
    hierarchy = {}
    for idx, row in organisation_hierarchy_df.iterrows():
        if user_only and row['source'] != 'user':
            continue
        h_group = row['h_group']
        sources[h_group] = row['source']
        hierarchy[h_group] = row['h_order'].split(',')
    
    return sources, hierarchy    

def get_non_user_dropdown(tenant_id, group_access_db):
    logging.info(f"######## Fetching Get Non User Dropdown")
    non_user_dropdown_query = select_star('non_user_dropdown')
    non_user_dropdown_df = group_access_db.execute_(non_user_dropdown_query)
    
    non_user_dropdown = {}
    database_dict = {"group_access" : group_access_db}
    db_config['tenant_id'] = tenant_id
    for idx, row in non_user_dropdown_df.iterrows():
        source = row['source']
        database, table = source.split('.')
        if database not in database_dict:
            database_dict[database] = DB(database, **db_config)
        table_columns_df = database_dict[database].execute_(f"SHOW COLUMNS FROM {table}")
        table_columns_list = list(table_columns_df['Field'])
        non_user_dropdown[source] = table_columns_list
    
    return non_user_dropdown


