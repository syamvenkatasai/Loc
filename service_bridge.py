"""
Author: Ashyam
Created Date: 20-02-2019
"""

import argparse
import os
import json
import requests
import re
import traceback
import subprocess
import jwt

from flask import Flask, request, jsonify, flash
from urllib.parse import urlparse
from time import time as tt
from db_utils import DB
import datetime
from datetime import datetime,timedelta
from app import app
import base64
import pandas as pd

try:
    from app.ace_logger import Logging
except:
    from ace_logger import Logging

logging = Logging(name='service_bridge')


db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def generate_token(apiSecret,apiKey):
    expiration_time = datetime.utcnow() + timedelta(minutes=30)
    payload = {
        'exp': expiration_time,
        'key': apiKey
    }
    token = jwt.encode(payload, apiSecret, algorithm='HS256')
    return token

@app.route('/generate_token_route', methods=['GET'])
def generate_token_route():
    start_time = tt()
    params_dict={}
    headers_dict={}
    try:
        args = request.args
        headers=request.headers
        for key,value in args.items():
            params_dict[key]=value
        for k,v in headers.items():
            headers_dict[k]=v
    except Exception as e:
        logging.exception(f"## Exception occured while reading headers")
    
    logging.info(f"## Headers got are {headers_dict}")
    apiSecret=headers_dict.get('Apisecret',None)
    apiKey=headers_dict.get('Apikey',None)
    if not apiSecret or not apiKey:
        return jsonify({'errorMessage': 'Invalid login details',"errorCode":1})
    
    try:
        token = generate_token(apiSecret,apiKey)
        end_time = tt()
        time_consumed = str(end_time-start_time)
        logging.info(f"Time Taken is {time_consumed}")
        logging.info(f"token generated is :{token}")
        return {'token': str(token),"errorCode": 0, "errorMessage": "Success"}
        
    except Exception as e:
        logging.exception(f" Exception Occured while generating token")
        return {"errorCode": 1, "errorMessage": "Invalid login details"}




@app.route('/servicebridge_health_check', methods=['GET'])
def servicebridge_health_check():

    return jsonify({'flag':True})

@app.route('/test', methods=['POST', 'GET'])
def test():
    try:
        data = request.json

        if data['data']:
            return jsonify({'flag': True, 'data': data})
        else:
            return jsonify({'flag': False, 'message': 'Failed to execute the function.'})
    except Exception as e:
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})
    

def is_valid_input(user_input):
    sql_injection_patterns = [
        r'\bDROP\b',
        r'\bINSERT\b',
        r'\bSELECT\b',
        r'\bUNION\b',
        r'\bWHERE\b',
        r'\b=\b',
        r'\bHTML\b',
        r'\bSCRIPT\b',
        r'\bGROUP BY\b',
        r'\bORDER BY\b',
        r'\bSLEEP\b',
        r'\bCONCAT\b',
        r'\bTRUNCATE\b'
    ]

    for pattern in sql_injection_patterns:
        
        if re.search(pattern, user_input, re.IGNORECASE):
           
            return False  

    return True  


def data_validtion(data):
    status=True
    if type(data)==dict:
        for key,value in data.items():
           
            if value=='NULL' or value==None or value=='None' or value=='null':
                status = True
            if type(value)==str:
                status=is_valid_input(value)
                
                if not status:
                    return status
            elif type(value)==list:
                for val in value:
                    status=data_validtion(val)
                    
                    if not status:
                        return status
            elif type(value)==dict:
                status=data_validtion(value)
                if not status:
                    return status
    return status

@app.route('/<route>', defaults={'argument': None}, methods=['POST', 'GET'])
@app.route('/<route>/<argument>', methods=['POST', 'GET'])
def connect(route, argument=None):
    """
    This is the only route called from the UI along with the name of the route in
    the URL and the data to be POSTed. This app will reroute to the corresponding
    route sent from the UI.
    The final URL will be generated using the bridge_config.json file. The route
    and its corresponding host and port will be stored in this file.

    Args:
        route (str): The route it should call this app should reroute to.
        data (dict): The data that needs to be sent to the URL.

    Returns:
        flag (bool): True if success otherwise False.
        message (str): Message for the user.

    Note:
        Along with the mentioned keys, there will be other keys that the called
        route will return. (See the docstring of the app)
    """
    try:
        logging.info('Serving a request')
        logging.info(f'Argument: {argument}')

        data = request.json
        logging.info(f'##########Data recieved: {data}')
        logging.info(f"### Going to execute data validation")

        route=data.get('route_name','')
        logging.info(f'Route: {route}')
        argument=data.get('argument','no_args')
        logging.info(f'Argument: {argument}')

        logging.debug('Reading bridge config')
        with open('/var/www/service_bridge/app/bridge_config.json') as f:
            connector_config = json.loads(f.read())

        if route not in connector_config:
            message = f'Invalid Request'
            logging.error(message)
            return jsonify({'flag': False, 'message': message})

        route_config = connector_config[route]

        host = route_config['host']
        port = route_config['port']
        port=443

        logging.debug(f'Host: {host}')
        logging.debug(f'Port: {port}')
            

        if request.method == 'POST':
            logging.debug(f'POST method. HEADERS ARE {request.headers}')
            headers_dict={}
            

            headers=request.headers
            for k,v in headers.items():
                logging.info(f"## Header key Val is {k}:{v}")
                headers_dict[k]=v
            bearer_token = headers_dict.get('Authorization',None)  
            logging.info(f"### Bearer got is {bearer_token}")
            try:
                tenant_id = data.get('tenant_id', '')
                db_config['tenant_id'] = tenant_id
                logging.info(f"tenant_id is {tenant_id}")
                session_id = data.get('session_id', '')
                session_db = DB('group_access', **db_config)
                logging.info(f"session_db is {session_db}")
                user = data.get('user', '')
                logging.info(f"user is {user}")
                logging.info("### Going to execute data validation")
                try:
                    # data_status=data_validtion(data)
                    data_status=True
                    session_db = DB('group_access', **db_config)
                    logging.info(f"### Final Data Status got is {data_status} and session_id is {session_id}")
                    query_session_id = "SELECT * FROM `live_sessions` WHERE status = 'active' AND user_ = %s AND session_id = %s"
                    output_session_id = session_db.execute_(query_session_id, params=[user, session_id])
                    logging.info(f"output_session_id is {output_session_id}")

                    if isinstance(output_session_id, pd.DataFrame) and not output_session_id.empty:
                        if session_id != output_session_id['session_id'].values[0]:
                            message = f'Session is not validated'
                            logging.info(f"message for session validation {message}")
                            return jsonify({'flag': False, 'message': message})
                    
                    if not data_status:
                            message = f'Invalid Data , Cannot Process Request'
                            return jsonify({'flag': False, 'message': message})
                    elif session_id=='' and route not in ('login','authentication_type'):
                        message = f'Empty session id'
                        logging.info(f"message for session empty is {message} and route is {route}")
                        return jsonify({'flag': False, 'message': message})
                    else:
                        pass
                    try:
                        tenant_id = data.get('tenant_id','')
                    except:
                        # logging.exception(f"Tenant is missing in request data: {data}")
                        # return jsonify({'flag': False, 'message': "Tenant Id is missing in request data"})
                        data['tenant_id'] = os.environ['TENANT_ID']
                except:
                    logging.warning('No data recieved.')
                    data = {}
               
                try:
                    
                    user = data.get('user', data.get('username', None))
                    login = data.get('login',False)
                    session_id = data.get('session_id','')
                    db_config['tenant_id'] = tenant_id
                    session_db = DB('group_access', **db_config)
                    
                    if session_id != '' and route not in ('login','authentication_type'):
                        query_session_id = "SELECT * FROM `live_sessions` WHERE status = 'active' AND user_ = %s AND session_id = %s"
                        output_session_id = session_db.execute_(query_session_id, params=[user, session_id]) 
                        logging.info(f"output_session_id is {output_session_id}")

                        if isinstance(output_session_id, pd.DataFrame) and not output_session_id.empty:
                            logging.info(f"Session is active: {output_session_id}")
                        else:
                            return jsonify({'flag': False, 'message': 'User or session not valid'})



                    if not data_status:
                        message = f'Invalid Data, Cannot Process Request'
                        return jsonify({'flag': False, 'message': message})
                    
                    query = f"SELECT * FROM `live_sessions` WHERE status = 'active' and user_ = '{user}' AND `session_id` != '{session_id}'"      
                    output = session_db.execute(query)
                    if not data_status:
                        pass
                        
                    elif not output.empty:
                        
                        session_id = list(output.session_id)[0]
                        update = f"update live_sessions set status = 'closed' where user_ = '{user}'"
                        session_db.execute(update)
                        
                        stats_db = DB('stats', **db_config) 
                        audit_data = {
                            "type": "insert", "last_modified_by": "service_bridge", "table_name": "live_sessions",
                            "reference_column": "user",
                            "reference_value": user, "changed_data": json.dumps({"status": "logout", "sessiontimeout": True, "session_id": session_id})
                        }
                        
                        stats_db.insert_dict(audit_data, 'audit_')
                        
                        
                    else:
                        update = f"update live_sessions set last_request = CURRENT_TIMESTAMP , status = 'active' where user_ = '{user}'"
                        session_db.execute(update)     
                except:
                    pass

                logging.debug(f'https://{host}:{port}/{route}/{argument}')
                
                def generate():
                    pem_file="/etc/ssl/custom_ca_bundle.pem"
                    if argument!='no_args':
                        print(F'with arguments')
                        logging.debug(f"arugument are >>>>>>>>>>> {argument}")
                        response = requests.post(f'https://{host}:{port}/{route}/{argument}', json=data,headers=headers_dict, stream=True,verify=False)
                        logging.info(f"**************************************************************{response}")
                        logging.info(f"*****************************************success*********************")
                    else:
                        logging.debug(f"There are no arguments to pass with route.>>>>>>>>>>>")
                        response = requests.post(f'https://{host}:{port}/{route}', json=data,headers=headers_dict, stream=True,verify=False)
                        logging.info(f"**************************************************************{response}")
                        logging.info(f"*****************************************success33333333333*********************")
                    for line in response.iter_lines():
                        yield line
                
                cache_clearing = ['usermanagement']

                if host in cache_clearing:
                    headers = {'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
                    
                    requests.post(f'https://queueapi:443/clear_cache', headers=headers,verify=False)

                return app.response_class(generate(), mimetype='text/html')
                
                
            except requests.exceptions.ConnectionError as e:
                message = f'ConnectionError: {e}'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})
            except Exception as e:
                message = f'Could not serve request.'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})
        elif request.method == 'GET':
            logging.debug('GET method.')
            try:
                params_dict={}
                headers_dict={}
                try:
                    args = request.args
                    headers=request.headers
                    for key,value in args.items():
                        params_dict[key]=value
                    for k,v in headers.items():
                        headers_dict[k]=v

                except Exception as e:
                    logging.info(f"############## Probably no args with url")
                    logging.exception(e)
                response = requests.get(f'https://{host}:{port}/{route}',params=params_dict,headers=headers_dict,stream=True,verify=False)
                logging.debug(f'Response: {response.content}')
                return jsonify(json.loads(response.content))
            except requests.exceptions.ConnectionError as e:
                message = f'ConnectionError: {e}'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})
            except Exception as e:
                message = f'Unknown error calling `{route}`. Maybe should use POST instead of GET. Check logs.'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})
    except Exception as e:
        logging.exception('Something went wrong in service bridge. Check trace.')
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})


@app.route('/zipkin', methods=['POST', 'GET'])
def zipkin():
    body = request.data
    requests.post(
            'http://zipkin:9411/api/v1/spans',
            data=body,
            headers={'Content-Type': 'application/x-thrift'},
        )
    return jsonify({'flag': True})



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-p', '--port', type=int,
                        help='Port Number', default=443)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')
    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, port=port, debug=False, threaded=True)