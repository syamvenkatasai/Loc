import os
import datetime
import json
import random
import requests
import time
import traceback

import psutil
import ldap
import string
import ast
from sqlalchemy.orm import sessionmaker


from flask import Flask, request, jsonify
from flask_cors import CORS
from base64 import b64decode
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2 
from hashlib import sha256

from py_zipkin.zipkin import zipkin_span,ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
from time import time as tt
from db_utils import DB
from datetime import timedelta as td

from ace_logger import Logging

from datetime import datetime
import pytz
tmzone = 'Asia/Kolkata'

from app import app


logging = Logging(name='user_auth_api')

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def http_transport(encoded_span):
    body = encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'})

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

def insert_into_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True

def generate_isac_ticket_no(isac_id,branch,operation,tenant_id,group_access_db,user_name,changed_by):
    part1=isac_id
    part2=branch
    part3=operation
    # Date part
    date_part = datetime.now().strftime('%d%b%Y').upper()
    
    # Random number part
    random_part = ''.join(random.choices(string.digits, k=6))
    
    # Combine all parts
    code = f'{part1}-{part2}-{part3}-{date_part}{random_part}'
    isac_data={"isac_ticket_no":code,"branch":branch,"operation":operation,"username":user_name,"changed_by":changed_by}
    group_access_db.insert_dict(isac_data,'isac_request_response')
    return code

def rand_sess_id():
    sid = 'S'
    for _ in range(3):
        sid = sid + str(random.randint(111, 999))
    # print("Starting session:",sid)
    return sid

def get_patch_version(tenant_id,db):

    """
        The person who is deploying need to update the table manually , after every deployment in the version control table in group access db
        purpose : to return the deployment version from the version control table
    """
    db_config['tenant_id']=tenant_id
    qry=f"SELECT `version` FROM `version_control` ORDER BY `last_update` DESC LIMIT 1"
    version_df = db.execute_(qry)
    version=version_df['version'].to_list()[0]
    return version


def ad_ldap_login(username, password,ad_ldap_info):
    """Verifies credentials for username and password.
    Returns None on success or a string describing the error on failure
    # Adapt to your needs
    """
    
    admin_un = ad_ldap_info['admin_username']
    admin_ps = ad_ldap_info['admin_password']
    basedn =ad_ldap_info['BASE_DN']
    LDAP_SERVER= ad_ldap_info['ldap_server']
    LDAP_USERNAME=username
    try:
        password=decrypt_passwd(password)
        LDAP_PASSWORD = password
        # initialize and perform a synchronous bind
        ldap_client = ldap.initialize(LDAP_SERVER)
    except Exception as e:
        logging.info(f"########## Error in initialising ldap client: {e}")
        logging.exception("#### Error ")

    try:
        response = ldap_client.simple_bind_s(LDAP_USERNAME, LDAP_PASSWORD)
    except ldap.INVALID_CREDENTIALS:
        ldap_client.unbind()
        logging.info('invalid password or username')
        return {'flag': False, 'message': 'invalid admin credentials'}
    except ldap.SERVER_DOWN:
        logging.info('AD server not available')
        return {'flag': False, 'message': 'AD server not available'}
    except:
        return {'flag': False, 'message': 'AD server not available'}

    try:
        results = ldap_client.search_s(basedn,ldap.SCOPE_SUBTREE,"(cn=*)")
    except:
        return {'flag': False, 'message': 'bind AD server with admin credentials failed'}

    for dn,entry in results:
        if 'sAMAccountName' in entry:
            if entry['sAMAccountName'][0].decode('utf-8') == LDAP_USERNAME:
                full_username = entry['userPrincipalName'][0].decode('utf-8')
                password = LDAP_PASSWORD
                try:
                    response = ldap_client.simple_bind_s(full_username, password)
                    logging.info('successfully authenticated')
                    return {'flag': True, 'message': 'successfully authenticated'}
                except ldap.INVALID_CREDENTIALS:
                    ldap_client.unbind()
                    logging.info('invalid password or username')
                    return {'flag': False, 'message': 'invalid password or username'}
                except ldap.SERVER_DOWN:
                    logging.info('AD server not available')
                    return {'flag': False, 'message': 'AD server not available'}

    return {'flag': True, 'message': 'User Not Found!!'}


@app.route('/userauth_api_health_check', methods=['POST', 'GET'])
def userauth_api_health_check():
    return jsonify({'flag':True})


def decrypt_passwd(decrypt_passwd):
    data = b64decode(decrypt_passwd)
    key1 = "acelogin"
    key2 = "logincheck"
    bytes = PBKDF2(key1.encode("utf-8"), key2.encode("utf-8"), 48, 128)
    iv = bytes[0:16]
    key = bytes[16:48]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    dec_psw = cipher.decrypt(data)
    dec_psw = dec_psw[:-dec_psw[-1]].decode("utf-8")
    logging.info(f'Password decrypted----{dec_psw}')

    return dec_psw

def ace_login(username, password, tenant_id):
    db_config['tenant_id'] = tenant_id
    db = DB('group_access', **db_config)

    try:
        users = db.get_all('active_directory')
        live_sessions = db.get_all('live_sessions')
    except Exception as e:
        message = f'Error fetching users from database: {e}'
        logging.exception(message)
        return {'flag': False, 'message': message}
    logging.info(f'####users_db = {users}')
    # Basic input/data sanity checks
    if users.empty:
        message = 'No users in the database.'
        logging.error(message)
        return {'flag': False, 'message': message}
    logging.info(f'####users.username.values = {users.username.values}')
    if username not in users.username.values:
        message = f'User Not Found.'
        logging.error(message)
        return {'flag': False, 'message': message}
    
    

    user = users.loc[users['username'] == username] # Get the user data
    logging.info(f'##user {user}')

    user_role=user.role.values[0]

    if user_role=='None' or user_role is None:
        message=f"{username} Unable to login due to role is in disabled status"
        return {'flag': False, 'message': message}

    #PBKDF2,AES decryption
    

    password=decrypt_passwd(password)


    # Verify entered password
    hashed_pw = sha256(password.encode()).hexdigest()
    user_pw = user.password.values[0]

    user_status_adm_query = f"SELECT STATUS FROM active_directory_modifications WHERE username = '{username}' and STATUS NOT IN ('approved','rejected')"
    user_status_adm_query = db.execute(user_status_adm_query)
    logging.info(f"####user_status_adm_query is {user_status_adm_query}")
    if user_status_adm_query.empty:
        #user status is taking from active directory
        user_status = user.status.values[0]
    else:
        #user status is taking from active directory modifications because the UAM Maker may update the user status
        user_status_adm = list(user_status_adm_query['STATUS'])
        logging.info(f"####user_status_adm is {user_status_adm}")
        user_status = user_status_adm[0]
    logging.info(f"user_status is {user_status}")
 
    user_login_attempts = user.login_attempts.values[0]
    logging.info(f'##user_status {user_status}')
    logging.info(f'##user_login_attempts {user_login_attempts}')

    if user_status == 'disable':
        message = 'Inactive user credentials'
        logging.error(message)
        return {'flag': False, 'message': message}
    #if user is deleted
    if user_status == 'delete':
        message = 'User is deleted'
        logging.error(message)
        return {'flag': False, 'message': message}
    #if user is locked
    if user_status == 'lock':
        message = 'User ID is locked'
        logging.error(message)
        return {'flag': False, 'message': message}
    #if user is waiting for UAM Checker approval
    if user_status == 'waiting':
        message = 'User Id is waiting for UAM Checker approval'
        logging.error(message)
        return {'flag': False, 'message': message}
    #if user is rejected by UAM Checker
    if user_status == 'rejected':
        message = 'User Id is rejected by UAM Checker'
        logging.error(message)
        return {'flag': False, 'message': message}
    #if user is id dormant status
    if user_status == 'dormant':
        message = 'User Id is in dormant status'
        logging.error(message)
        return {'flag': False, 'message': message}
    if hashed_pw != user_pw:
        user_login_attempts = user_login_attempts - 1
        if user_login_attempts == 0:
            message = 'Exceeding incorrect password attempts'
            logging.error(message)
            lock_query = f"""update active_directory set status='lock', login_attempts=0 where username='{username}'"""
            db.execute(lock_query)
            return {'flag': False, 'message': message}
        else:
            message = f'Password Incorrect only {user_login_attempts} attempts left'
            logging.error(message)
            logging.info(f'##user_login_attempts {user_login_attempts}')
            login_attempts_query = f"""update active_directory set login_attempts={user_login_attempts} where username='{username}'"""
            logging.info(f'##user_login_attempts_query {login_attempts_query}')
            db.execute(login_attempts_query)
            return {'flag': False, 'message': message}

    logging.info(f"###user_role is {user_role}")
    if user_role in ("UAM Maker","UAM Checker","UAM Reviewer"):
        return {'flag': True, "display_usermanagement":True ,'message': 'successfully authenticated' }

    return {'flag': True, "display_usermanagement":False ,'message': 'successfully authenticated' }
     

def check_user_status(username,db):
    status=True
    username=username
    status_qry="select `status` from `live_sessions` where `user`= %s "
    get_status_df=db.execute_(status_qry, params = [username])
    user_status=get_status_df['status'].to_list()[0]
    if user_status=='active':
        update_qry = "UPDATE `live_sessions` SET `status`= %s, `logout`= %s WHERE `user` = %s"
        current_ist = datetime.now(pytz.timezone(tmzone))
        currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
        params = ['closed', currentTS, username]
        db.execute(update_qry, params=params)
        status=True
    else:
        pass
    return status


@app.route('/login', methods=['POST', 'GET'])
def login():
    """
    Log in to ACE.

    Args:
        username (str): Username of the user.
        password (str): Password of the user.

    Returns:
        flag (bool): True if success otherwise False.
        message (str): Message for the user.
        data (dict): User details from the database. This key will not be present
            if any error occurs during the process.
    """
    content = request.get_json(force=True)
    logging.info(f"Request Data: {content}")
    data_toreturn={}
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = content.pop('tenant_id', None)
    session_id = content.pop('session_id', None)
    
    trace_id=generate_random_64bit_string()
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id if tenant_id is not None else ''
    )
    
    with zipkin_span(
            service_name='generate_report',
            zipkin_attrs=attr,
            span_name='generate_report',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:

        username = content.pop('username', None)
        if username:
            username = username.lower()
        password = content.pop('password', None)

        db_config['tenant_id'] = tenant_id
        
        db = DB('group_access', **db_config)
        timedelta = os.environ['TIMEDELTA']
        timedelta = int(timedelta)
        logging.info(f'##timedelta - {type(timedelta)},{timedelta}')
        try:
                        
            query = f"select status,previous_status from active_directory where username='{username}'"
            res = db.execute_(query)
            status = res['status'][0]
            prev_status = res['previous_status'][0]
            try:
                prev_status = ast.literal_eval(prev_status)
            except:
                prev_status = prev_status

            if status == 'enable' and prev_status[-1] == 'dormant':
                
                query = f"""select systimestamp - LAST_UPDATED as time from active_directory where username='{username}'"""
                last_request_df = db.execute_(query)
                logging.info(f'######################timedelta - {last_request_df}')
            
                last_request_df = str(last_request_df.at[0,'time'])
                last_request_df = last_request_df.split(' ')
                last_request_df = int(last_request_df[0])
                if last_request_df>=1:
                    prev_status_update = json.dumps(prev_status[:-1])
                    lock_query = f"""update active_directory set status='dormant',previous_status='{prev_status_update}' where username='{username}'"""
                    db.execute_(lock_query)
                    message = 'User is in dormant'
                    return {'flag': False, 'message': message}
                prev_status_update = json.dumps(prev_status[:-1])
                lock_query = f"""update active_directory set previous_status='{prev_status_update}' where username='{username}'"""
                db.execute_(lock_query)

            else:

                live_sessions_users_query = f"select user_ as users from live_sessions"
                live_sessions_users_df = db.execute_(live_sessions_users_query)                
                live_session_users = list(live_sessions_users_df['users'])
                live_session_users = [elem.lower() if elem is not None else None for elem in live_session_users]

                if username.lower() not in live_session_users:
                    query = f"""select systimestamp - CREATED_DATE as time from active_directory where username='{username}'"""
                    last_request_df = db.execute_(query)
                else:
                    query = f"""select systimestamp - LAST_REQUEST_NEW as time from live_sessions where user_='{username}'"""
                    last_request_df = db.execute_(query)

                last_request_df = str(last_request_df.at[0,'time'])
                last_request_df = last_request_df.split(' ')
                last_request_df = int(last_request_df[0])
                if last_request_df>timedelta:
                    lock_query = f"""update active_directory set status='dormant' where username='{username}'"""
                    db.execute_(lock_query)
                    message = 'User is dormant or locked.'
                    return {'flag': False, 'message': message}
            

        except Exception as e:
            print(f'########### error in Dormating - Locking user {e}')
        
        try:
            query = 'select * from `login_type`'
            login_type_df = db.execute(query)
            
            if list(login_type_df.loc[:,'ace'])[0]:
                return_data = ace_login(username, password, tenant_id)
            elif list(login_type_df.loc[:,'sso'])[0]:
                if list(login_type_df.loc[:,'sso_type'])[0] == 'AD_LDAP':
                    return_data = ad_ldap_login(username, password)
            
        except Exception as e:
            message = f'Error fetching login type from database: {e}'
            logging.exception(message)
            return {'flag': False, 'message': message}

        if return_data['flag']:
            try:
                users = db.get_all('active_directory')
                live_sessions = db.get_all('live_sessions')
                logging.info(f'##live_sessions {live_sessions}')    
            except Exception as e:
                message = f'Error fetching users from database: {e}'
                logging.exception(message)
                return {'flag': False, 'message': message}
        
            # Basic input/data sanity checks
            if users.empty:
                message = 'No users in the database.'
                logging.error(message)
                return {'flag': False, 'message': message}

            if username not in users.username.values:
                message = f'User Not Found.'
                logging.error(message)
                return {'flag': False, 'message': message}

            user = users.loc[users['username'] == username] # Get the user data
            logging.info(f"####user is {user}")
            session_id = rand_sess_id()

           

            hashed_pw=password
            user_pw=password
            user_status = user.status.values[0]
            role=user.role.values[0]
            theme=user.selectedtheme.values[0]

            if user_status == 'disable':
                message = 'Inactive user credentials'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})
            if hashed_pw != user_pw:
                message = 'Password Incorrect.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            #changing login attempts to 3
            login_attempts_query = f"""update active_directory set login_attempts=3 where username='{username}'"""
            db.execute(login_attempts_query)

            if user.iloc[0]['status'] and username not in list(live_sessions.loc[live_sessions['status'] == 'active'].user):
                data = json.loads(user.drop(columns='password').to_json(orient='records'))[0]
                data_toreturn['session_id'] = session_id
                data_toreturn['tenant_id']=tenant_id
                data_toreturn["username"]=username
                data_toreturn["role"]=role
                data_toreturn["selectedtheme"]=theme

                logging.info(f'###live_sessions.user.values {live_sessions.user.values} username is {username}')
                if username not in live_sessions.user.values:
                    insert = f"""INSERT INTO live_sessions(user_, session_id, status) VALUES ('{username}', '{session_id}', 'active')"""
                    logging.info(f'###insert query {insert}')
                    db.execute(insert)
                
                else:
                    update = "UPDATE live_sessions SET session_id=%s, status=%s, logout=%s , login=CURRENT_TIMESTAMP, LAST_REQUEST_NEW=CURRENT_TIMESTAMP  WHERE `user_`=%s"
                    params = [session_id, 'active', None, username]
                                    

                return_data = {'flag': True, "display_usermanagement": return_data["display_usermanagement"] ,'data': data_toreturn}


            elif user.iloc[0]['status'] and username in live_sessions.loc[live_sessions['status'] == 'active'].user.values:
                message = 'Logged in succesfully. Previous session has been terminated'
                logging.info(message)
                data = json.loads(user.drop(columns='password').to_json(orient='records'))[0]
                
                data_toreturn['session_id'] = session_id
                data_toreturn['tenant_id']=tenant_id
                data_toreturn["username"]=username
                data_toreturn["role"]=role
                data_toreturn["selectedtheme"]=theme
                user_id = list(user.index.values)[0]
               
                try:
                    last_login=f"""select Login from live_sessions where user_='{username}'"""
                    last_login_query=db.execute(last_login)
                    last_login=str(last_login_query['login'][0])
                    last_login_time = datetime.strptime(last_login, '%Y-%m-%d %H:%M:%S.%f')

                    # Define the timedelta representing 5 hours and 30 minutes
                    offset = td(hours=5, minutes=30)

                    # Add the offset to the last_login time to get the IST time
                    ist_time = last_login_time + offset

                    # Convert IST time back to string if needed
                    last_login_status = ist_time.strftime('%Y-%m-%d %H:%M:%S')
                    logging.info(f'##last_login {last_login}')
                except Exception as e:
                    message = f'Error fetching users from database: {e}'
                    logging.exception(message)
                    return {'flag': False, 'message': message}
                data_toreturn['last_login_status']=last_login_status
                current_ist = datetime.now(pytz.timezone(tmzone))
                currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
                update = "UPDATE live_sessions SET session_id=%s,login=CURRENT_TIMESTAMP, LAST_REQUEST_NEW=CURRENT_TIMESTAMP WHERE user_=%s"
                params = [session_id, username]
                db.execute(update, params=params)
                

                insert_login_logout = """INSERT INTO LOGIN_LOGOUT(user_name, login, session_id) VALUES(:user_name, TO_TIMESTAMP(:login, 'YYYY-MM-DD HH24:MI:SS'), :session_id)"""
                params = {'user_name': username, 'login': currentTS, 'session_id': session_id}
                db.execute(insert_login_logout, params=params)

                
                return_data = {'flag': True, "display_usermanagement": return_data["display_usermanagement"] ,'data': data_toreturn ,'idle_timeout': 5 }
                logging.info(f"return_data is {return_data}")
            else:
                message = 'User is inactive.'
                return_data = {'flag': False, 'message': message, 'data':{}}

        elif "status" in return_data:
            message = 'User has already logged in.'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"]=='User is deleted':
            message = 'User is deleted'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"]=='User ID is locked':
            message = 'User ID is locked'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"] == 'User Id is waiting for UAM Checker approval':
            message = 'User Id is waiting for UAM Checker approval'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"] == 'User Id is rejected by UAM Checker':
            message = 'User Id is rejected by UAM Checker'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"]=='Exceeding incorrect password attempts':
            message = 'User ID is locked due to exceeding incorrect password attempts'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"]=='User Id is in dormant status':
            message = 'User is in dormant'
            return {'flag': False,'message': message,'data':{}}
        elif return_data["message"]=='Inactive user credentials':
            message = 'Inactive user credentials'
            return {'flag': False,'message': message,'data':{}}
        else:
            message = return_data["message"]
            logging.info(message)
            return_data = {'flag': False, 'message': message, 'data':{}}
        
        
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
        logging.info(f"---------------------------------------username is ,{username}")
        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": username,
                        "api_service": "login", "service_container": "user_auth_api", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(content), 
                        "response_data": json.dumps(return_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(return_data['flag'])}
        insert_into_audit(audit_data)
        try:
            return_data['data'].pop('password',None)
            return_data['data'].pop('PASSWORD',None)
        except:
            logging.info(f"error at removing the fields in the login response")
            pass
        return jsonify(return_data)

@app.route('/logout', methods=['POST', 'GET'])
def logout():
    # Database configuration
    content = request.get_json(force=True)
    logging.info(f"Request Data: {content}")
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = content.get('tenant_id', None)
    user = content.get('username', None)
    session_id = content.get('session_id', None)
    theme = content.get('selectedTheme', None)
    trace_id = generate_random_64bit_string()
    db_config['tenant_id'] = tenant_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(service_name='user_auth_api', span_name='logout',
                transport_handler=http_transport, port=5003, sample_rate=0.05,):

        try:
            db = DB('group_access', **db_config)
            queue_db = DB('queues', **db_config)
            stats_db = DB('stats', **db_config)

            engine = db.engine
            Session = sessionmaker(bind = engine)
            session = Session()

            current_ist = datetime.now(pytz.timezone(tmzone))
            currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
            currentts_logout = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

            
            update = f"UPDATE LIVE_SESSIONS SET STATUS = 'closed', LOGOUT = '{currentts_logout}', LAST_REQUEST_NEW = '{currentts_logout}' WHERE USER_ = '{user}' AND SESSION_ID = '{session_id}'"
            
            res = db.execute(update)
            logging.info(f"####update query is {update} ###res is {res}")
            
            
            update_query = "UPDATE LOGIN_LOGOUT SET LOGOUT = TO_TIMESTAMP(:logout, 'YYYY-MM-DD HH24:MI:SS')  WHERE USER_NAME = :user_name AND SESSION_ID = :session_id"
            params = {'logout': currentTS, 'user_name': user, 'session_id': session_id}

            db.execute(update_query, params=params)

            message = 'Logged out successfully'
            logging.info(message)
            
            query = "UPDATE `active_directory` SET `selectedTheme`= %s WHERE `username` = %s"
            params = [theme, user]
            db.execute(query, params=params)

            session.commit()
            
            """
            audit_data = {
              "type": "insert", "last_modified_by": "user_auth_api", "table_name": "live_sessions",
                "reference_column": "user",
                "reference_value": username, "changed_data": json.dumps({"status": "logout", "session_id": session_id})
            }
            stats_db.insert_dict(audit_data, 'audit')
            """

            response_data={'flag': True, 'message': message,'data':{}}
        except Exception:
            message = 'Error logging out.'
            logging.exception(message)
            response_data={'flag': False, 'message': message,'data':{}}
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

        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user, 
                        "api_service": "logout", "service_container": "user_auth_api", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(content), 
                        "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(audit_data)
        logging.info(f'######response_data is {response_data}')

    return response_data



@app.route('/change_password', methods=['POST', 'GET'])
def change_password():
    content = request.get_json(force=True)
    logging.info(f"Request Data: {content}")
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = content.get('tenant_id', None)
    user = content.get('username', None)
    session_id = content.get('session_id', None)
    
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
        span_name='change_password',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
        try:
            # Database configuration
            old_password = content.pop('old_password')
            new_password = content.pop('new_password')
            username = content.pop('username')
            db_config['tenant_id'] = tenant_id
            db = DB('group_access', **db_config)
            users = db.get_all('active_directory')

            # Basic input/data sanity checks
            if users.empty:
                message = 'No users in the database.'
                logging.error(message)
                response_data={'flag': False, 'message': message}

            if username not in users.username.values:
                message = 'User Not Found.'
                logging.error(message)
                response_data={'flag': False, 'message': message}

            user = users.loc[users['username'] == username] # Get the user data

            # Verify entered password
            hashed_pw = sha256(old_password.encode()).hexdigest()
            user_pw = user.password.values[0]
            if hashed_pw != user_pw:
                message = 'Password Incorrect.'
                logging.error(message)
                reponse_data = {'flag': False, 'message': message}
            else:
                hashed_npw = sha256(new_password.encode()).hexdigest()
                query = f"update active_directory set password = '{hashed_npw}' where username = '{username}'"
                db.execute(query)

                response_data = {'flag': True, 'message': "Successfully changed password","data":{}}
        except Exception as e:
            message = f"Something went wrong in change of password--{e}"
            response_data = {}
            logging.exception(message)
            response_data = {"flag":False, "message":message,"data":response_data}
        
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

        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": username,
                        "api_service": "change_password", "service_container": "user_auth_api", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(content), 
                        "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(audit_data)

        return jsonify(response_data)


@app.route('/change_status', methods=['POST', 'GET'])
def change_status():
    data = request.json
    logging.info(f"Request Data: {data}")
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', None)
    user = data.get('username', None)
    session_id = data.get('session_id', None)

    changed_by=data.get('initiatedBy',None)
    approved_by=data.get('approvedBy',None)
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
        span_name='change_status',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
        try:
            username = data.pop('username', None)
            status = data.pop('status', 'disable')

            previous_status_=data.get('previous_status',["enable"])
          
            role=data.get('role',None)
            
            
            db_config['tenant_id'] = tenant_id

            db = DB('group_access', **db_config)

            try:
                user_id=data.get("id",None)
            except Exception as e:
                get_id=f"select id from active_directory where username='{username}'"
                user_id=db.execute_(get_id)['id'].to_list()[0]
                data['id']=user_id

            logging.info(f'###status  {status}')
            
            data_active_directory_query = f"""
                                            SELECT USERNAME, 
                                                CASE 
                                                    WHEN USERNAME = '{username}' THEN STATUS
                                                    ELSE NULL
                                                END AS STATUS , BRANCH_CODE
                                            FROM active_directory
                                            """
            data_active_directory_df = db.execute_(data_active_directory_query)
            usernames = list(data_active_directory_df['USERNAME'])
            usernames_list = [elem.lower() for elem in usernames if elem is not None]
            user_status = data_active_directory_df.loc[data_active_directory_df['USERNAME'] == username, 'STATUS'].values[0] if username in data_active_directory_df['USERNAME'].values else None
            user_branch = data_active_directory_df.loc[data_active_directory_df['USERNAME'] == username, 'BRANCH_CODE'].values[0] if username in data_active_directory_df['USERNAME'].values else None
            logging.info(f'#######user_status is = {user_status},  usernames_list = {usernames_list}')
            ## generating isac token
            try:
                operation=status[0:1].upper()
                isac_token=generate_isac_ticket_no('1',user_branch,operation,tenant_id,db,username,changed_by)
            except Exception as e:
                logging.exception(f" Error occured while generationg isac token number")
                isac_token=''
                pass
            if username.lower() not in usernames_list:
                message=f"User id does not exist"
                response_data = {'flag': False,'errorCode':2,'isacTicketNo':isac_token,'message': message}
            elif status == 'delete' and user_status == 'delete':
                message=f"User id already deleted"
                response_data = {'flag': False,'errorCode':3,'isacTicketNo':isac_token, 'message': message}
            elif status == 'disable' and user_status == 'disable':
                message=f"User id already in disabled status"
                response_data = {'flag': False, 'errorCode':4,'isacTicketNo':isac_token,'message': message}
            elif status == 'enable' and user_status == 'enable':
                message=f"User id already in enable status"
                response_data = {'flag': False,'errorCode':6,'isacTicketNo':isac_token, 'message': message}
            elif status == 'unlock' and user_status == 'delete':
                message=f"User id already deleted"
                response_data = {'flag': False,'errorCode':8,'isacTicketNo':isac_token, 'message': message}
            elif status == 'unlock' and user_status in ('unlock','disable'):
                message=f"User id not locked"
                response_data = {'flag': False,'errorCode':9,'isacTicketNo':isac_token, 'message': message}
            elif status == 'revoke' and user_status in ('enable','disable','lock','unlock'):
                message=f"User id is in active status"
                response_data = {'flag': False,'errorCode':7,'isacTicketNo':isac_token, 'message': message}
            else:

                active_users_query = f"SELECT USERNAME FROM active_directory_modifications where STATUS NOT IN ('approved','rejected')"
                active_directory_df = db.execute(active_users_query)
                users_list = list(active_directory_df['USERNAME'])
                users_list = [elem.lower() for elem in users_list if elem is not None]
                if username.lower() in users_list:
                    return {"flag": False,"errorCode":2, "message" : 'Record already sent for verification',"isacTicketNo":isac_token}
                else:
                    #User deleted date & User disabled date adding
                    current_ist = datetime.now(pytz.timezone(tmzone))
                    currentTS = current_ist.strftime('%Y-%m-%d %H:%M:%S')
                    current_date = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

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



                    #Adding status to audit for history
                    query = f"select * from active_directory where username='{username}'"
                    res = db.execute(query)
                    created_date = res['created_date'][0]
                    user_audit = res['user_audit'][0]
                    user_id = res['id'][0]
                    final_list = []
                    logging.info(f'User audit is: {user_audit}')
                    res = res.to_dict(orient='records')
                    logging.info(f"####res is {res}")

                    created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')
                    

                    l_list = []
                    l = []
                    dic = {}
                    dic['user_id'] = str(user_id)
                    dic['field_name'] = "status"
                    dic['old_value'] = res[0]['status']
                    dic['new_value'] = status
                    dic['modified_date'] = currentTS
                    dic['created_date'] = created_date
                    dic['modified_by'] = changed_by
                    l.append(dic)

                    l_list = l
                    final_list = l_list

                    final_list = json.dumps(final_list)

                    logging.info(f"###final_list is {final_list}")

                    query = f"update active_directory set user_audit='{final_list}' where username='{username}'"
                    logging.info(f"###query is {query}")
                    db.execute(query)




                    #Latest modified code for UAM role
                    ad_query = f"""SELECT ADM.user_email,ADM.role,ADM.username,ADM.password,
                        TO_CHAR(ADM.created_date, 'DD-MON-YY HH.MI.SS.FF6 AM') created_date,
                        TO_CHAR(ADM.last_updated, 'DD-MON-YY HH.MI.SS.FF6 AM') last_updated,
                        ADM.selectedtheme,ADM.employee_code,ADM.employee_id,ADM.employee_name,ADM.branch_code,
                        ADM.branch_name,ADM.department_code,ADM.department_name,ADM.role_code,ADM.role_name,ADM.mobile_no,ADM.address,
                        ADM.supervisor_code,ADM.status,ADM.login_attempts,ADM.previous_status,ADM.old_employee_name,ADM.old_role_name,ADM.old_branch_name,ADM.created_user,
                        TO_CHAR(ADM.DELETED_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') deleted_date,
                        TO_CHAR(ADM.USER_MODIFIED_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') user_modified_date,
                        TO_CHAR(ADM.USER_DISABLED_DATE, 'DD-MON-YY HH.MI.SS.FF6 AM') user_disabled_date,
                        ADM.isac_ticket_no, ADM.isac_ticket_status
                    FROM 
                        hdfc_group_access.active_directory ADM WHERE USERNAME = '{username.lower()}'"""
                    ad_df = db.execute(ad_query)
                    logging.info(f"#####ad_df is {ad_df}")

                    if ad_df.empty:
                        logging.info('No data fetched from the database')
                    else:
                        # Convert the DataFrame to a dictionary
                        user_data_dict_data = ad_df.to_dict(orient='records')[0]
                        user_data_dict = {}

                        for key, value in user_data_dict_data.items():
                            lower_key = key.lower()
                            if lower_key not in user_data_dict:
                                user_data_dict[lower_key] = value

                        logging.info(f'update_user_data result----> {user_data_dict}')
                    
                    if status == 'delete':
                        user_data_dict['deleted_date'] = current_date
                        user_data_dict['user_disabled_date'] = ''
                    elif status == 'disable':
                        user_data_dict['deleted_date'] = '31-DEC-49 11.59.59.000000000 PM'
                        user_data_dict['user_disabled_date'] = current_date
                    else:
                        user_data_dict['deleted_date'] = '31-DEC-49 11.59.59.000000000 PM'
                        user_data_dict['user_disabled_date'] = ''

                    user_data_dict['status'] = status
                    user_data_dict['previous_status'] = json.dumps(previous_status_)
                    user_data_dict['role'] = role
                    user_data_dict['last_updated'] = current_date
                    user_data_dict['isac_ticket_no']=isac_token
                    user_data_dict['maker_id'] = changed_by
                    user_data_dict['user_modified_date'] = current_date
                    user_data_dict['last_updated'] = current_date

                    try:

                        maker_username = changed_by
                        maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
                        maker_checker_ids_query = db.execute(maker_checker_ids_query)
                        maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
                        logging.info(f"####maker_checker_employee_names is {maker_checker_ids}")
                        maker_name = maker_checker_ids[0]
                        logging.info(f"maker_name is {maker_name}")

                        user_data_dict['maker_name'] = maker_name
                        user_data_dict['maker_date'] = current_date
                    except Exception as e:
                        user_data_dict['maker_name'] = changed_by
                        user_data_dict['maker_date'] = current_date




                    logging.info(f'user_data_dict after modifications ----> {user_data_dict}')

                    # Remove entries with None values
                    filtered_data_ad = {k: v for k, v in user_data_dict.items() if v is not None}
                    col_list = ', '.join(filtered_data_ad.keys())
                    #val_list = ', '.join(f"'{v}'" for v in filtered_data_ad.values())
                    val_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data_ad.values())

                    inserting_query = f"INSERT INTO active_directory_modifications ({col_list}) VALUES ({val_list})"
                    try:
                        updated_data = db.execute(inserting_query)
                        logging.info(f'update_data result----> {updated_data}')

                        role_select_query = f"SELECT EMPLOYEE_NAME,BRANCH_CODE,BRANCH_NAME,ROLE,OLD_EMPLOYEE_NAME,OLD_ROLE_NAME,OLD_BRANCH_NAME FROM active_directory WHERE ID = '{user_id}'"
                        role_select_query_data = db.execute_(role_select_query)
                        logging.info(f"####role_select_query_data is = {role_select_query_data}")

                        user_old_role = role_select_query_data['ROLE'].iloc[0]
                        if not user_old_role or user_old_role == "None" or user_old_role == None:
                            user_old_role = role_select_query_data['OLD_ROLE_NAME'].iloc[0]

                        maker_username = changed_by
                        maker_checker_ids_query = f"SELECT EMPLOYEE_NAME FROM `active_directory` WHERE `username` = '{maker_username}'"
                        maker_checker_ids_query = db.execute_(maker_checker_ids_query)
                        maker_checker_ids = list(maker_checker_ids_query['EMPLOYEE_NAME'])
                        logging.info(f"####maker_checker_ids is {maker_checker_ids}")
                        maker_name = maker_checker_ids[0]
                        logging.info(f"maker_name is {maker_name}")

                        
                        data_to_insert = {
                            'ID':user_id,
                            'USERNAME': username,
                            'EMPLOYEE_NAME': role_select_query_data['EMPLOYEE_NAME'].iloc[0],
                            'BRANCH_CODE': role_select_query_data['BRANCH_CODE'].iloc[0],
                            'BRANCH_NAME': role_select_query_data['BRANCH_NAME'].iloc[0],
                            'ROLE': role,
                            'ACTIVITY': status,
                            'STATUS':status,
                            'MAKER_ID': changed_by,
                            'CREATED_USER': maker_name,
                            'USER_MODIFIED_DATE': current_date,
                            'LAST_UPDATED_DATE': current_date
                        }

                        filtered_data = {k: v for k, v in data_to_insert.items() if v not in ('',None)}
                        columns_list = ', '.join(filtered_data.keys())
                        #values_list = ', '.join(f"'{v}'" for v in filtered_data.values())
                        values_list = ', '.join(f"'{str(v).replace("'", "''")}'" for v in filtered_data.values())

                        insert_query = f"INSERT INTO USER_OPERATIONS ({columns_list}) VALUES ({values_list})"
                        logging.info(f'#####insert_query is {insert_query}')
                        query_result = db.execute_(insert_query)
                        logging.info(f'#####query_result is {query_result}')
                        
                        if role is None or role == 'null' or role=="None":
                            if status=='enable' or status=='revoke':
                                message=f"{username} Unable to login due to role is in disabled status"
                            else:
                               
                                if status == 'enable':
                                    message = 'User has been activated and sent for verification'
                                elif status == 'disable':
                                    message = 'User has been deactivated and sent for verification'
                                elif status == 'lock':
                                    message = 'User has been locked and sent for verification'
                                elif status == 'delete':
                                    message = 'User has been deleted and sent for verification'
                                elif status == 'revoke':
                                    message = 'User has been revoked and sent for verification'
                                elif status == 'unlock':
                                    message = 'User has been unlocked and sent for verification'
                                elif status == 'dormant':
                                    message = 'User has been dormanted and sent for verification'
                                else:
                                    message = f"Record sent for varification"
                        else:
                           
                            if status == 'enable':
                                message = 'User has been activated and sent for verification'
                            elif status == 'disable':
                                message = 'User has been deactivated and sent for verification'
                            elif status == 'lock':
                                message = 'Usr has been locked and sent for verification'
                            elif status == 'delete':
                                message = 'User has been deleted and sent for verification'
                            elif status == 'revoke':
                                message = 'User has been revoked and sent for verification'
                            elif status == 'unlock':
                                message = 'User has been unlocked and sent for verification'
                            elif status == 'dormant':
                                message = 'User has been dormanted and sent for verification'
                            else:
                                message = f"Record sent for varification"

                        response_data = {'flag': True,'errorCode':0,'isacTicketNo':isac_token, 'message': message}

                    except Exception as e:
                        logging.error(f'Error executing insert query: {e}')

        except Exception as e:
            message = 'error in changing the user status'
            logging.info(f"## Exception occured ..{e}")
            response_data = {'flag': False,'errorCode':0,'isacTicketNo':isac_token, 'message': message}
        
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
                        "response_data": json.dumps(response_data['message']), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(audit_data)

        return jsonify(response_data)


@app.route('/authentication_type', methods=['POST', 'GET'])
def authentication_type():  # sourcery skip: avoid-builtin-shadow
    try:
        data = request.json
        
        tenant_id = data.get('tenant_id', 'kmb')

        db_config['tenant_id'] = tenant_id
        db = DB('group_access', **db_config)

        query = "select * from login_type"
        login_type = list(db.execute_(query).to_dict(orient='records'))[0]
        ace = bool(login_type['ace'])
        sso = bool(login_type['sso'])
        if ace:
            type = 'ace'
        elif sso:
            type = 'sso'

        else:
            logging.debug("login type table is not defined properly")

        try:
            ## this function is responsible to return the version
            version=get_patch_version(tenant_id,db)
        except Exception as e:
            version=''
            pass

        # to get the password in encrypted format
        return jsonify({'flag': True, 'type':type,'encrypt_pass' : True,'version':version})

    except Exception as e:
        message = 'error in database connection'
        return jsonify({'flag': False, 'message': message})
