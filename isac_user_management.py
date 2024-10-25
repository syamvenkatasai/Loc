
import json
import requests
import traceback
import os
import sqlalchemy
import pandas as pd
import psutil
import jwt

from db_utils import DB
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
import datetime
from datetime import datetime
import pytz
tmzone = 'Asia/Kolkata'


logging = Logging(name='user_management')

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

def insert_into_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True

def edit_user_isac(user_id,row_data, group_access_db, user,isac_token):
    try:
        user_id=user_id
        user_name = row_data.get('userId',None)
        initiated_by = row_data.get('initiatedBy', None)
        approved_by = row_data.get('approvedBy', None)
        attributes = row_data.get('attributes', [])
    except:
        traceback.print_exc()
        message = "id not present in request data."
        return {"errorCode":2, "errorMessage" : 'Invalid user details',"isacTicketNo":isac_token} 
      
    if not isac_token:
        return {"errorCode":2, "errorMessage" : 'isacTicketNo is required'}
    
    engine = group_access_db.engine
    Session = sessionmaker(bind = engine)
    session = Session()
    g_db_no_autocommit = group_access_db
    
    try:

        logging.info(f'###user id is {user_id}')
        logging.info(f'###row data is {row_data}')
        role =row_data.get('role', None)

                 
        data_active_directory_query = f"select EMPLOYEE_ID, EMPLOYEE_CODE, EMPLOYEE_NAME, USER_EMAIL, USERNAME, ROLE from active_directory where ID <> '{user_id}'"
        data_active_directory_df = group_access_db.execute_(data_active_directory_query)
        logging.info(f'#######data_active_directory_df = {data_active_directory_df}')
        # employee_ids = list(data_active_directory_df['EMPLOYEE_ID'])
        # employee_ids = [elem.lower() if elem is not None else None for elem in employee_ids]
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
            # message=f"User id does not exist"
            return {"errorCode":2, "errorMessage" : 'Invalid User ID',"isacTicketNo":isac_token}   
            # return {'flag': False, 'message': message}

        res = ''

        if row_data['employeeCode'].lower() in employee_codes:
            res = res+'employee_code '
        if row_data['emailId'].lower() in user_emails:
            res = res+'user_email '
        if row_data['userId'].lower() in usernames:
            res = res+'user_id '
        logging.info(f'##res = {res}')
        if res != '':
            res = 'Duplicate '+res.replace(' ', ', ').strip(', ')
            message = res
            logging.info(f'##res = {res}, message = {message}')
            return {"errorCode":1, "errorMessage" : 'User ID Already Exists',"isacTicketNo":isac_token}
       
        def escape_apostrophe(field_value):
            if field_value:
                if "'" in field_value:
                    field_value = field_value.replace("'", "''")
                elif "'\\''" in field_value:
                    field_value = field_value.replace("'\\''", "''")
                elif "\\'" in field_value:
                    field_value = field_value.replace("\\'", "''")
            return field_value
        
        set_clause_arr = []

        prev_data_query = f"SELECT STATUS,EMPLOYEE_NAME, ROLE, BRANCH_NAME, OLD_EMPLOYEE_NAME, OLD_ROLE_NAME, OLD_BRANCH_NAME FROM active_directory WHERE id = '{user_id}'"
        prev_data = group_access_db.execute(prev_data_query)
        logging.info(f'####prev_data {prev_data}')
        prev_employee_name = list(prev_data['employee_name'])[0]
        prev_branch_name = list(prev_data['branch_name'])[0]
        prev_role = list(prev_data['role'])[0]
        user_status=list(prev_data['status'])[0]
        
        old_employee_name = ''
        old_branch_name = ''
        old_role_name = ''
        new_employee_name = ''
        new_branch_name = ''
        new_role_name = ''

        #Employee name is not mandatory for edit user this is client request, so adding employee name to row_data
        if row_data['employeeName'] == '' or not row_data['employeeName']:
            prev_employee_name = escape_apostrophe(prev_employee_name)
            row_data['employeeName'] = prev_employee_name
        if row_data['branchName'] == '' or not row_data['branchName']:
            row_data['branchName'] = prev_branch_name

        #row data
        role_name = row_data['attributes'][0]['role']
        branch_name = row_data['branchName']
        employee_name = row_data['employeeName']

        if user_status=='delete':
            return {"errorCode":13, "errorMessage" : f'user ID is deleted cannot be modified',"isacTicketNo":isac_token}

        if user_status not in ['enable' ,'revoke']:
            return {"errorCode":5, "errorMessage" : f'user is in {user_status} state',"isacTicketNo":isac_token}
        

        if employee_name != prev_employee_name:
            old_employee_name = prev_employee_name
            new_employee_name = employee_name
        if branch_name != prev_branch_name:
            old_branch_name = prev_branch_name
            new_branch_name = branch_name

        if role_name != prev_role:
            old_role_name = prev_role
            new_role_name = role_name
        else:
            return {"errorCode":5, "errorMessage" : 'Role already Mapped to User',"isacTicketNo":isac_token}
        
        #if the role is empty need to give previous role like role = ""
        if row_data['role'] == '' or not row_data['role']:
            row_data['role'] = prev_role
            role_name = row_data['attributes'][0]['role'] = prev_role

        logging.info(f'old_employee_name={old_employee_name}, old_branch_name={old_branch_name}, old_role_name={old_role_name}')
        
        #Handling the apostrophy
        
        if old_employee_name:
            old_employee_name=escape_apostrophe(old_employee_name)
        
        old_details = []
        if old_employee_name and old_employee_name not in ("None","NONE","none"):
            old_details.append(f"OLD_EMPLOYEE_NAME = '{old_employee_name}'")
        if old_branch_name and old_branch_name not in ("None","NONE","none"):
            old_details.append(f"OLD_BRANCH_NAME = '{old_branch_name}'")
        if old_role_name and old_role_name not in ("None","NONE","none"):
            old_details.append(f"OLD_ROLE_NAME = '{old_role_name}'")
        if old_details:
            old_details_string = ', '.join(old_details)
            old_details_query = f"UPDATE active_directory SET {old_details_string} WHERE id = '{user_id}'"
            #update_data_query = f"UPDATE active_directory SET OLD_EMPLOYEE_NAME='{old_employee_name}', OLD_ROLE_NAME='{old_role_name}', OLD_BRANCH_NAME='{old_branch_name}' WHERE id = '{user_id}'"
            update_data = group_access_db.execute(old_details_query)
            logging.info(f'update_data result----> {update_data}')

        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

        data_to_insert = {
            'ID': str(user_id),
            'USERNAME': row_data['userId'],
            'EMPLOYEE_NAME': row_data['employeeName'],
            'BRANCH_CODE': row_data['branchCode'],
            'BRANCH_NAME': row_data['branchName'],
            'ROLE': row_data['attributes'][0]['role'],
            'ACTIVITY': 'Modify',
            'STATUS' : 'Modify',
            'MAKER_ID': initiated_by,
            'CREATED_USER': initiated_by,
            'USER_MODIFIED_DATE': currentTS,
            'CHECKER_ID': approved_by,
            'CHECKER_NAME': approved_by,
            'CHECKER_DATE': currentTS,
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
        
        column_mapping = {
            'id':'ID',
            'emailId': 'USER_EMAIL',
            'role': 'ROLE',
            'userId': 'USERNAME',
            'password': 'PASSWORD',
            'employeeCode': 'EMPLOYEE_CODE',
            'employeeName': 'EMPLOYEE_NAME',
            'branchCode': 'BRANCH_CODE',
            'branchName': 'BRANCH_NAME',
            'departmentCode': 'DEPARTMENT_CODE',
            'departmentName': 'DEPARTMENT_NAME',
            'isac_token': 'ISAC_TICKET_NO',  
            'user': 'CREATED_USER', 
            'approvedBy': 'APPROVED_BY'
        }
        active_directory_data={}

        for key,val in row_data.items():
            try:
                if val:
                    active_directory_data[column_mapping[key]]=val
                else:
                    pass
            except:
                pass

        for set_column, set_value in active_directory_data.items():
            if set_column!="attributes":
                set_clause_arr.append(f"{set_column} = '{set_value}'")
    
        set_clause_arr.append(f"CREATED_USER = '{user}'")
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
        modified_date = f"TO_TIMESTAMP('{currentTS}', 'YYYY-MM-DD HH24:MI:SS')"
        set_clause_arr.append(f"USER_MODIFIED_DATE = {modified_date}")

        set_clause_arr.append(f"MAKER_ID = '{initiated_by}'")
        set_clause_arr.append(f"MAKER_NAME = '{initiated_by}'")
        set_clause_arr.append(f"MAKER_DATE = {modified_date}")
        set_clause_arr.append(f"CHECKER_ID = '{approved_by}'")
        set_clause_arr.append(f"CHECKER_NAME = '{approved_by}'")
        set_clause_arr.append(f"CHECKER_DATE = {modified_date}")


        set_clause_string = ', '.join(set_clause_arr)

        logging.info(f" SET CLAUSE STRING IS {set_clause_string}")
            
        query = f"UPDATE active_directory SET {set_clause_string} WHERE id = '{user_id}'"
        result = session.execute(query)
        logging.info(f'result-----------{result}')
        session.commit()
        
        
        if not result:
            session.rollback()
            logging.warning('rolling back')
            session.close()
            logging.warning('closing session')
            message = f"Something went wrong while updating the user {user_name} | user_id {user_id} in active directory"
            return {"errorCode":2, "errorMessage" : 'Invalid User ID',"isacTicketNo":isac_token}
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while updating the user {user_name} | user_id {user_id} in active directory"
        return {"errorCode":2, "errorMessage" : 'Invalid User ID',"isacTicketNo":isac_token}
    
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
        return {"errorCode":1, "errorMessage" : 'Invalid User ID',"isacTicketNo":isac_token}    
    
    
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
        return {"errorCode":3, "errorMessage" : 'Invalid Role/Group',"isacTicketNo":isac_token}
   
    result = generate_insert_user_org_mapping(user_id, row_data, group_access_db) 
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
            return {"errorCode":3, "errorMessage" : 'Invalid Role/Group',"isacTicketNo":isac_token}
    
    except:
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while deleting the user {user_name} | user_id {user_id} from user_organisation_mapping"
        return {"errorCode":3, "errorMessage" : 'Invalid Role/Group',"isacTicketNo":isac_token}
    try:    
        insert_query = generate_multiple_insert_query(to_insert, 'user_organisation_mapping')
        result = session.execute(insert_query)
        
        if not result:
            message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping"
            return {"errorCode":3, "errorMessage" : 'Invalid Role/Group',"isacTicketNo":isac_token}
    except:
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping"
        return {"errorCode":3, "errorMessage" : 'Invalid Role/Group',"isacTicketNo":isac_token}
  
    
    session.commit()
    session.close()
    message = f"Successfully updated {user_name} | user_id {user_id} in database"
    message="Success"
    return {"errorCode":0, "errorMessage" :message,"isacTicketNo":isac_token}


def create_user_isac(row_data, group_access_db, queue_db, user,isac_token):
    # TRY USING COMMIT AND ROLLBACK WITH SQLALCHEMY
    orig_row_data = row_data.copy()
    try:
        if user:
            row_data['CREATED_USER'] = user
        row_data['role'] = row_data['attributes'][0]['role']
        attributes = row_data.pop('attributes', [])
        
        initiated_by = row_data.get('initiatedBy', None)
        approved_by = row_data.get('approvedBy', None)
        user_name = row_data['userId'].lower()
        if "password" not in row_data:
            row_data['password']='1234'
        else:
            pass
        logging.info(f"row_data received : {row_data}")
    except:
        traceback.print_exc()
        message = "id not present in request data."
        return {"errorCode":10, "errorMessage" : 'Invalid user details',"isacTicketNo":isac_token}
    
    engine = group_access_db.engine
    g_db_no_autocommit = group_access_db
    
    Session = sessionmaker(bind = engine)
    session = Session()
    
    try:
        
        query = f'select max(id) as id from active_directory'
        id_ = group_access_db.execute_(query)['id'][0]
        id_ = id_+1
        print(f'Row Data is: {row_data}')
        data_active_directory_query = f"select EMPLOYEE_ID, EMPLOYEE_CODE, EMPLOYEE_NAME, USER_EMAIL, USERNAME, ROLE from active_directory"
        data_active_directory_df = group_access_db.execute_(data_active_directory_query)
        logging.info(f'#######data_active_directory_df = {data_active_directory_df}')
        # employee_ids = list(data_active_directory_df['EMPLOYEE_ID'])
        # employee_ids = [elem.lower() if elem is not None else None for elem in employee_ids]
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

        res = ''
        
        if row_data['employeeCode'] and row_data['employeeCode'].lower() in employee_codes:
            res = res+'employee_code '

        # if row_data['emailId'] and row_data['emailId'].lower() in user_emails:
        #     res = res+'user_email '
        if row_data['userId'] and row_data['userId'].lower() in usernames:
            res = res+'user_id '

        logging.info(f'##res = {res}')
        if res != '':
            res = 'Duplicate '+res.replace(' ', ', ').strip(', ')
            message = res
            logging.info(f'##res = {res}, message = {message}')
            message='User ID already exists'
            return {"errorCode":1, "errorMessage" : message,"isacTicketNo":isac_token}
        
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
        row_data['USER_MODIFIED_DATE'] = currentTS        

        row_data['password'] = sha256(row_data['password'].encode()).hexdigest()
        
        ## modifying row data as per the column names in the active directory
        
        active_directory_data={
            'ID': str(id_),
            'USER_EMAIL':row_data.get('emailId',None),
            'ROLE':row_data.get('role',None),
            'USERNAME':row_data.get('userId',None).lower(),
            'PASSWORD':row_data.get('password',None),
            'EMPLOYEE_CODE':row_data.get('employeeCode',None),
            'EMPLOYEE_NAME':row_data.get('employeeName',None),
            'BRANCH_CODE':row_data.get('branchCode',None),
            'BRANCH_NAME':row_data.get('branchName',None),
            'DEPARTMENT_CODE':row_data.get('departmentCode',None),
            'DEPARTMENT_NAME':row_data.get('departmentName',None),
            'ISAC_TICKET_NO':isac_token,
            'CREATED_USER':user,
            'CREATED_DATE': currentTS,
            'APPROVED_BY':row_data.get('approvedBy',None),
            'MAKER_ID': initiated_by,
            'MAKER_NAME': initiated_by,
            'MAKER_DATE': currentTS,
            'CHECKER_ID': approved_by,
            'CHECKER_NAME': approved_by,
            'CHECKER_DATE': currentTS
        }

        logging.info(f"### ROW DATA IS {row_data} \n ACTIVE DIRECTORY DATA IS {active_directory_data}")
        
        create_user_query = generate_insert_query(active_directory_data, 'active_directory')
        logging.info(f'create_user_query----{create_user_query}')
        session.execute(create_user_query)
        session.commit()

        query = f"SELECT * FROM `active_directory` WHERE `username` = '{user_name}'"
        active_directory_df = group_access_db.execute_(query)
        user_id = list(active_directory_df['id'])[0]



        #Adding data to user operations table
        data_to_insert = {
            'ID':str(user_id),
            'USERNAME': row_data['userId'],
            'EMPLOYEE_NAME': row_data['employeeName'],
            'BRANCH_CODE': row_data['branchCode'],
            'BRANCH_NAME': row_data['branchName'],
            'ROLE': row_data['role'],
            'ACTIVITY': 'Create',
            'STATUS' : 'Create',
            'CREATED_USER': initiated_by,
            'USER_MODIFIED_DATE': currentTS,
            'MAKER_ID': initiated_by,
            'CHECKER_ID': approved_by,
            'CHECKER_NAME': approved_by,
            'CHECKER_DATE': currentTS,
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
        

        
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = "Duplicate entry for username"
        return {"errorCode":1, "errorMessage" : 'User id already exists',"isacTicketNo":isac_token}
    except:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        # message = "Something went wrong while creating user_name {user_name}."
        message="Invalid user ID"
        return {"errorCode":2, "errorMessage" : 'User id already exists',"isacTicketNo":isac_token}
    
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
         
        message="Invalid Role/Group"
        return {"errorCode":3, "errorMessage" :message,"isacTicketNo":isac_token}  
    
    
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
            message="Invalid Role/Group"
            return {"errorCode":3, "errorMessage" :message,"isacTicketNo":isac_token}  
    
    else:
        traceback.print_exc()
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message="Invalid Role/Group"
        return {"errorCode":3, "errorMessage" :message,"isacTicketNo":isac_token}  
    
    result = generate_insert_user_org_mapping(user_id, orig_row_data, group_access_db) 
    to_insert = result['data'] if result['flag'] else []

    logging.info(f"### generate_insert_user_org_mapping RESULT IS {result}")

    
    logging.info(f"#################333 TO_INSERT: {to_insert}")

    if to_insert:
        try:    
            insert_query = generate_multiple_insert_query(to_insert, 'user_organisation_mapping')
            logging.info(f"################# USER ORG MAPPING INSERT QUERY IS  {insert_query}")
            result = session.execute(insert_query)
            
            if not result:
                session.rollback()
                logging.warning('rolling back')
                session.close()
                logging.warning('closing session')
                # message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping"
                # return {"flag": False, "message" : message}
                message="Invalid Role/Group"
                return {"errorCode":3, "errorMessage" :message,"isacTicketNo":isac_token}  
    
            session.commit()
            logging.warning('committing session')
            session.close()
            logging.warning('session closed')
            
            message="Success"
            return {"errorCode":0, "errorMessage" :message,"isacTicketNo":isac_token}  
    
        except Exception as e:
            traceback.print_exc()
            session.rollback()
            logging.warning('rolling back')
            session.close()
            logging.warning('closing session')
            message = f"Something went wrong while inserting details for user {user_name} | user_id {user_id} in user_organisation_mapping. {e}"
           
            return {"errorCode":2, "errorMessage" : 'Invalid user details',"isacTicketNo":isac_token} 
    
    else:
        session.rollback()
        logging.warning('rolling back')
        session.close()
        logging.warning('closing session')
        message = f"No data found for user {user_name} | user_id {user_id} to insert in user_organisation_mapping"
         
        return {"errorCode":2, "errorMessage" : 'Invalid user details',"isacTicketNo":isac_token}   


def generate_insert_user_org_mapping(user_id, rowdata, group_access_db):
    try:
        to_insert = []
        attributes_list = rowdata['attributes']
        sequence_id = 1
        for attributes in attributes_list:
            
            username=rowdata['userId']

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


def generate_insert_query(dict_data, table_name, db = "mysql"):
    columns_list,values_list = [],[]
    logging.debug(f"dict_data: {dict_data}")

    for column, value in dict_data.items():
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