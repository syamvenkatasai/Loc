
import json
import os
from numpy import extract
import requests
import psutil
import traceback
import html
import pandas as pd
from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
from time import time as tt
from db_utils import DB
from ace_logger import Logging
from datetime import datetime
from app import app
import numpy as np
import re
logging = Logging(name='button_functions')

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
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


def insert_into_audit(case_id, data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True


@zipkin_span(service_name='folder_monitor', span_name='produce_with_zipkin')
def produce_with_zipkin(first_route, data):
    logging.info('Producing with zipkin...')

    zipkin_headers = create_http_headers_for_new_span()
    data['zipkin_headers'] = zipkin_headers
    logging.debug(f'Zipkin data: {data}')

    produce(first_route, data)


def get_task_id_by_index(tab_id, sub_queue_list, sub_queue_task_id_list):

    logging.info(f"####### tab_id: {tab_id}")
    logging.info(f"###### sub_queue_list: {sub_queue_list}")
    logging.info(f"###### sub_queue_task_id_list: {sub_queue_task_id_list}")

    queue_list = json.loads(sub_queue_list)
    task_id = ""

    if type(queue_list) == list:
        tab_index = queue_list.index(tab_id)
        task_id = json.loads(sub_queue_task_id_list)[tab_index]

    return task_id


def send_email_alert(tenant_id, template):
    host = os.environ.get('HOST_IP', "")
    data = {"tenant_id": tenant_id, "template": template}
    logging.debug(f"Data sending to email trigger is {data}")
    try:
        headers = {'Content-type': 'application/json; charset=utf-8'}
        logging.info("Entering to mail############")
        email_response = requests.post(
            f'http://{host}:5002/send_email', json=data, headers=headers)
        logging.info(f"####### EMAIL API Response: {email_response.text}")
    except Exception as e:
        traceback.print_exc()
        logging.info(f"Mail Failed {e}")
    return True

@app.route('/execute_button_function', methods=['POST', 'GET'])
def execute_button_function():
    data = request.json
    logging.info(f"Request Data of Template Detection: {data}")

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    document_id = data.get('document_id', '')
    move_to = data.get("group", "default")

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='button_functions',
            span_name='execute_button_function',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        
        user__ = user
        variables = data.get("variables", {})
        fields = data.get("fields", {})
        original_case_id = data.get('case_id', None)
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction', **db_config)

        

        if tenant_id == 'ambanketrade':
            camunda_host = data.get('camunda_host', 'camundaworkflowtrade')
            camunda_port = data.get('camunda_port', '8080')
            camunda_port = int(camunda_port)

        if tenant_id == 'ambankdisbursement':
            camunda_host = data.get('camunda_host', 'camundaworkflowdisb')
            camunda_port = data.get('camunda_port', '8080')
            camunda_port = int(camunda_port)

        if tenant_id == 'kmb' or tenant_id == 'hdfc':
            camunda_host = data.get('camunda_host', 'camundaworkflow')
            camunda_port = data.get('camunda_port', '8080')
            camunda_port = int(camunda_port)
    


        if 'retrain' in data.keys():
            host = 'trainingapi'
            port = 80
            route = 'train'
            logging.debug(f'Hitting URL: http://{host}:{port}/{route}')
            headers = {
                'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
            response = requests.post(
                f'http://{host}:{port}/{route}', json=data, headers=headers)
            response = response.json()
            if response['flag']:
                return jsonify({'flag': True, 'data': {'message': 'Training completed!'}})
            else:
                msg = response['message']
                logging.exception('error in loading the training')
                return jsonify({'flag': False, 'message': msg})
        
        if move_to == 'Approve':
            try:
                host = 'foldermonitor'
                port = 443
                route = 'dp_formula_calculation'
                logging.debug(f'Hitting URL: https://{host}:{port}/{route}')
                logging.debug(f'Sending Data: {data}')
                headers = {'Content-type': 'application/json; charset=utf-8',
                        'Accept': 'text/json'}
                response = requests.post(
                    f'https://{host}:{port}/{route}', json=data, headers=headers,verify=False)

                logging.info(f"in dp formula execution")
            except:
                message = '###issue in dp formula'
                logging.exception(message)


        try:
            queue_id = data.get('queue_id', None)
            
            tab_id = data.get('tab_id', None)


            update = {
                'last_updated_by': user__,
            }
            where = {
                'case_id': original_case_id
            }

            if original_case_id:
                queue_db.update('process_queue', update=update, where=where)

            queue_name = ''

            if queue_id:
                queue_name = list(queue_db.execute_(
                    f'select unique_name from queue_definition where id = {queue_id}')['unique_name'])[0]

            

            task_id = ""
            if tab_id is None:
                query_queue_list = f"select task_id from queue_list where case_id = '{case_id}' and queue='{queue_name}'"
                task_list = queue_db.execute_(query_queue_list).task_id
                if len(task_list) > 0:
                    task_id = list(queue_db.execute_(
                        query_queue_list).task_id)[0]
                else:
                    logging.info(
                        f"############ task id is empty for queue: {queue_name} and case id: {case_id}")

            else:

                

                if "variables" in data:
                    if "email_alert" in data["variables"]:
                        logging.debug(
                            "email alert is present in variables of button")

                        template = data["variables"]["email_alert"]

                        logging.debug("sending to send email alert mail")
                        flag = send_email_alert(tenant_id, template)

                    if "tab_id" in data["variables"]:
                        logging.info(f"Found tab id")
                        tab_id = data["variables"]["tab_id"]

                logging.info(f"##################333 Tab id found is {tab_id}")
                sub_queue_query = f"select sub_queue_list,sub_queue_task_id_list from process_queue where case_id='{case_id}'"
                sub_queue_df = queue_db.execute_(sub_queue_query)
                sub_queue_list = sub_queue_df["sub_queue_list"][0]
                sub_queue_task_id_list = sub_queue_df["sub_queue_task_id_list"][0]

                task_id = get_task_id_by_index(
                    tab_id, sub_queue_list, sub_queue_task_id_list)
           
            
            if move_to == 'Cancel':
                query_pq = 'UPDATE `process_queue` SET `case_lock`=0 WHERE `case_id`=%s'
                queue_db.execute(query_pq, params=[case_id])
                return jsonify({'flag': True})
            """if move_to == 'Submit Unhold Comments':
                
                return jsonify({'flag': True})
            if move_to == 'Submit Hold Comments':
                
                return jsonify({'flag': True})
            
            if move_to == 'Re-Apply':
                
                return jsonify({'flag': True})"""
            
            

            if "variables" in data:
                
                if "case_status" in data["variables"]:
                    update['status'] = data["variables"]["case_status"]

                if "screen_name" in data["variables"]:
                    move_to = move_to + ' ' + data["variables"]["screen_name"]


            api_params = {"variables": {"button": {"value": move_to},
                                        "ui_data": {"value": data}}, "case_id": case_id}

            logging.debug(f'API Params: {api_params}')

            logging.debug(
                f'Hitting URL: http://{camunda_host}:{camunda_port}/rest/engine/default/task/{task_id}/complete')
            headers = {'Content-type': 'application/json; charset=utf-8'}

            request_post_ = requests.post(
                f'http://{camunda_host}:{camunda_port}/rest/engine/default/task/{task_id}/complete', json=api_params, headers=headers , timeout=1500)

            request_post = request_post_.json
            logging.info(f"##########request_post is {request_post} ")

            """if request_post_.status_code == 500:
                message = json.loads(request_post_.content)
                message = message['message'].split(':')[0]
                if message == case_id:
                    message = "Something went wrong while saving the data! Please try again."
                response_data = {'flag': False, 'message': message, 'data': {
                "button_name": move_to}}"""
            if request_post_.status_code == 500:
                message = json.loads(request_post_.content)
                logging.info(f"Camunda sent 500 the message is: {message}")
                message = message['message'].split(':')[0]
                message = f"{message}: Case is in use by multiple users"
                logging.info(f"Final message: {message}")
                if message == case_id:
                    message = "Something went wrong while saving the data! Please try again."
                response_data = {'flag': False, 'message': message, 'data': {
                    "button_name": move_to}}

                query = f"select pre_task_id from queue_list where case_id = '{case_id}'"
                pre_task_id = queue_db.execute_(query)['pre_task_id'].to_list()[0]
                print(f"pre_task_id is {pre_task_id}")

                logging.debug(f'hitting with pre task id')

                update2 = f"UPDATE queue_list SET task_id = '{pre_task_id}' WHERE case_id = '{case_id}'"
                
                queue_db.execute_(update2)

                logging.debug(
                    f'Hitting URL: http://{camunda_host}:{camunda_port}/rest/engine/default/task/{pre_task_id}/complete')

                requests.post(f'http://{camunda_host}:{camunda_port}/rest/engine/default/task/{pre_task_id}/complete', json=api_params, headers=headers)


            
            response_data = {'flag': True, 'data': {"status": "Button executed successfully", "button_name": move_to}}
            
            queue_db.update('process_queue', update=update,
                            where={'case_id': case_id})
            
        except KeyError as e:
            logging.exception(
                'Something went wrong executing button functions. Check Trace.')
            response_data = {'flag': False, 'message': 'something went wrong', 'data': {
                "button_name": move_to}}

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            time_consumed = str(end_time-start_time)
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time, 3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id,
                      "api_service": "execute_button_function", "service_container": "button_functions", "changed_data": None,
                      "tables_involved": "", "memory_usage_gb": str(memory_consumed),
                      "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
                      "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": session_id, "status": str(response_data['flag'])}
        insert_into_audit(case_id, audit_data)

        return jsonify(response_data)