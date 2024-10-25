import os
import argparse
import datetime
import json
import random
import requests
import psutil
import time
import traceback
import shutil

from flask import Flask, request, jsonify
from flask_cors import CORS
from time import time as tt
from hashlib import sha256
from py_zipkin.zipkin import zipkin_span,ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
from pathlib import Path

from db_utils import DB
from ace_logger import Logging
from .case_migration import *
#from producer import produce


from app import app


logging = Logging(name='camunda_api')

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

def insert_into_audit(case_id, data):
    logging.info(f"{data} ##data")
    tenant_id = data.pop('TENANT_ID')
    logging.info(f"{tenant_id} ##data")
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'AUDIT_')
    return True

@app.route('/camunda_api_health_check', methods=['GET'])
def camunda_api_health_check():

    return jsonify({'flag':True})

@app.route('/update_queue', methods=['POST', 'GET'])
def update_queue():
    data = request.get_json(force=True)
    logging.info(f"Request Data {data}")

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', None)
    # case_id = data["email"]["case_id"]
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    document_id = data.get('document_id', '')
    camunda_host = data.get('camunda_host','camundaworkflow')
    camunda_port = data.get('camunda_port','8080')
    
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    if (user is None) or (session_id is None):
        ui_data = data.get('ui_data', {'user':user,'session_id':None})
        user = ui_data.get('user', None)
        session_id = ui_data.get('session_id', None)

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='camunda_api',
        span_name='update_queue',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        try:
            flow_list = data.get('flow_list', None)

            if flow_list is None:
                logging.info('###################### Flow list empty, creating new')
                flow_list = []
            
            flow_list_in_json = []
            for i in flow_list:
                flow_list_in_json.append(json.loads(i))

            queue = data.get('queue', None)
            task_id = data.get('task_id', None)
            ui_data = data.get('ui_data',{})
        
            db_config['tenant_id'] = tenant_id

            db = DB('queues', **db_config)

            #Unlocking the case for other Button functions to execute
            query = 'UPDATE `process_queue` SET `case_lock`=0 WHERE `case_id`=%s'
            db.execute_(query, params=[case_id])
            try:
                query = f"select task_id from queue_list where case_id = '{case_id}'"
                pre_task_id=db.execute_(query)['task_id'].to_list()[0]
                print(F"pre_task_id is {pre_task_id}")
            except:
                pre_task_id='0'
                pass


            
            
            update1 = f"update process_queue set `completed_processes` = NULL,`flow_list` = '{json.dumps(flow_list_in_json)}' where case_id = '{case_id}' "
            qs_w_task_id_query = f"select count(*) as count from queue_list where case_id = '{case_id}'"

            qs_w_task_id = int(db.execute_(qs_w_task_id_query)["count"])

            update2=""

            if "previous_queue" in data:
                p_queue = data["previous_queue"]
                if p_queue=="" or p_queue is None:
                    p_queue="default"
            
                if p_queue == "default":
                    update2 = f"""insert into queue_list (case_id, queue, task_id, parallel) values ('{case_id}','{queue}', '{task_id}', 1)"""
                elif p_queue != "default":
                    update2 = f"update queue_list set queue = '{queue}', task_id = '{task_id}' ,pre_task_id = '{pre_task_id}' where case_id = '{case_id}' and queue =  '{p_queue}'"
            

            else:
                if qs_w_task_id == 0:

                    update2 = f"""insert into queue_list (case_id, queue, task_id, parallel) values ('{case_id}','{queue}', '{task_id}',0)"""

                else:
                    update2 = f"update queue_list set queue = '{queue}', task_id = '{task_id}', pre_task_id = '{pre_task_id}' where case_id = '{case_id}' "


            logging.info(f"$$$$$$$$$$$$$$$4 {update1} and {update2}")

            db.execute_(update1) and db.execute_(update2)
        
            response_data = {}
            try:
                query = f"select `document_id` from `process_queue` where case_id='{case_id}'"
                document_id=db.execute_(query)
                document_id=document_id['document_id'][0]
            except:
                document_id=''
            response_data['message']='Successfully updated queue '
            response_data['case_id']=case_id
            
            response_data['camunda_host']=camunda_host
            response_data['camunda_port']=camunda_port
            response_data['updated_queue'] = queue

            if "previous_queue" in data:
                response_data["previous_queue"] = data["queue"]

            
            response_data = {'flag': True,'data':response_data }

        except Exception as e:
            message = f'error in database connection'
            logging.exception(e)
            response_data = {'flag': False, 'message': message, 'data':{}}
        
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
        
        
        audit_data={
                    "TENANT_ID": tenant_id,
                    "USER_": user,
                    "CASE_ID": case_id,
                    "API_SERVICE": "update_queue",
                    "INGESTED_QUEUE": queue,
                    "SERVICE_CONTAINER": "camunda_api",
                    "CHANGED_DATA": None,
                    "TABLES_INVOLVED": "",
                    "MEMORY_USAGE_GB": str(memory_consumed),
                    "TIME_CONSUMED_SECS": time_consumed,
                    "REQUEST_PAYLOAD": json.dumps(data),
                    "RESPONSE_DATA": json.dumps(response_data['data']),
                    "TRACE_ID": trace_id,
                    "SESSION_ID": session_id,
                    "STATUS": str(response_data['flag'])
                }
        logging.info(f"{audit_data}#####audit_data")

        insert_into_audit(case_id, audit_data)

        return jsonify(response_data)






def fetch_camunda_db_xml(tenant_id):

    db_config['tenant_id'] = tenant_id

    queues_db = DB('queues', **db_config)

    #Unlocking the case for other Button functions to execute
    try:
        query = 'select `diagrams` from `camunda_xml`'
        camunda_xml_list=list(queues_db.execute_(query)['diagrams'])

        if len(camunda_xml_list)>0:
            logging.info(f"########### Camunda XML from DB: {camunda_xml_list}")
            camnunda_xml_list = json.loads(camunda_xml_list[0])

            return camnunda_xml_list

        else:
            return []

    except Exception as e:
        logging.info("######### Error in fetching camunda xml from DB")
        logging.exception(e)

@app.route('/deploy_workflow_from_db', methods=['POST', 'GET'])
def deployWorkflow_from_db():
    try:
        request_data = request.get_json(force=True)
        #data = request_data.get("data",None)
        tenant_id = request_data.get("tenant_id",None)

        given_workflow_name = request_data.get("workflow_name","")
        camunda_port = request_data.get("camunda_port","8080")

        os_camundaworkflow_host='camundaworkflow'

        host = os.environ['SERVER_IP']

        endpoint = f'http://{os_camundaworkflow_host}:{camunda_port}/rest/engine/default/deployment/create'
    
        response_json={}
        deployment_flag=True
        deployment_message = "Deployement is Successfull "

        data=fetch_camunda_db_xml(tenant_id)
        # data=db_camunda_xml_list.get("data",None)

        for each_workflow in data:  

            file_type = each_workflow["type"]
            workflow_xml = each_workflow["diagram"]
            workflow_name=each_workflow["name"]

            if given_workflow_name=="":
                pass
            elif given_workflow_name!=workflow_name:
                continue

            logging.info("##############"+str(file_type))
            
            logging.info("##############"+str(workflow_name))


            with open("/temp_folder/"+workflow_name+"."+file_type,"w+", encoding='utf-8') as f:
                f.write(workflow_xml)

            files = {"upload": open("/temp_folder/"+workflow_name+"."+file_type, 'rb')}
            logging.info(f"#############{files}")

            
            response = requests.post(endpoint, files=files)
            if response.status_code==200:
                response_json[workflow_name]=True
                
            else:
                response_json[workflow_name]=False
                deployment_flag=False
                deployment_message = "All/Few deployements were unsucessfull"
        
        
        response_data = {'flag':deployment_flag,'message':deployment_message,'data':response_json}
    
        return jsonify(response_data)

    except Exception as e:
        logging.exception(e)
        message = "Error in Deployment Workflow"
        return jsonify({'flag': False, 'message': message})
                                                         
@app.route('/complete_task', methods=['POST', 'GET'])
def complete_task():

    if request.method == 'GET':

        try:
            camunda_url = request.args.get('camunda_url')
            logging.info(
                f"########### Camunda URL for completion: {camunda_url}")
            response = requests.post(camunda_url, json={})

            if response.status_code == 204:

                return jsonify({'flag': True, 'data': {'message': 'Completed the task Successfully'}})
            else:
                return jsonify({'flag': False, 'message': f'Camunda returned an error{response.content}'})

        except Exception as e:
            logging.info(
                f"############# Error in posting to camunda for Task Completion")
            logging.exception(e)
            return jsonify({'flag': False, 'message': f'Error in posting completion query to Camunda'})


@app.route('/get_bpmn_names', methods=['POST', 'GET'])
def get_bpmn_names():

    try:

        flag, response = get_bpmn_list()

        if flag:
            return(jsonify({"flag": True, "data": {"bpmns": response}}))

        else:

            return(jsonify({"flag": False, "message": response}))

    except Exception as e:
        logging.info(f"####### Error in geeting camunda bpmn list: {e}")

        return(jsonify({"flag": False, "message": "Could not get the bpmn list from camunda"}))


@app.route('/get_bpmn_versions', methods=['POST', 'GET'])
def get_bpmn_versions():

    data = request.get_json(force=True)

    bpmn_name = data.get("bpmn_name", "")

    if bpmn_name == "":
        return jsonify({"flag": False, "message": "please send the bpmn name to get versions"})

    try:

        flag, response = get_bpmn_process_versions(bpmn_name)

        if flag:
            return(jsonify({"flag": True, "data": {"bpmns": response}}))

        else:

            return(jsonify({"flag": False, "message": response}))

    except Exception as e:
        logging.info(f"####### Error in geeting camunda bpmn Versions: {e}")

        return(jsonify({"flag": False, "message": "Could not get the bpmn versions from camunda"}))


@app.route('/get_bpmn_xml_tokens', methods=['POST', 'GET'])
def get_bpmn_xml_tokens():

    data = request.get_json(force=True)

    bpmn_name = data.get("bpmn_version", "")

    if bpmn_name == "":
        return jsonify({"flag": False, "message": "please send the bpmn version to get tokens and xml"})

    try:

        flag, response = get_tokens_xml(bpmn_name)

        if flag:
            return(jsonify({"flag": True, "data": response}))

        else:

            return(jsonify({"flag": False, "message": response}))

    except Exception as e:
        logging.info(f"####### Error in geeting camunda bpmn Versions: {e}")

        return(jsonify({"flag": False, "message": "Could not get the bpmn versions from camunda"}))


@app.route('/case_migration', methods=['POST', 'GET'])
def case_migration():

    data = request.get_json(force=True)

    source_version = data.get("source_version", "")
    dest_version = data.get("dest_version", "")

    flag = data.get("flag", "")

    if source_version == "" or dest_version == "" or source_version == dest_version or flag == "":
        return jsonify({"flag": False, "message": "please send valid bpmn versions to migrate"})

    try:

        if flag == "validate":

            valid_flag, valid_response = validate_migration_plan(
                source_version, dest_version)

            logging.info(f"########## Validation Response: {valid_response}")

            return(jsonify({"flag": valid_flag, "data": valid_response}))

        elif flag == "migrate":

            migrate_flag, migrate_response = migrate_cases(
                source_version, dest_version)

            if migrate_flag:
                return jsonify({"flag": True, "data": {"message": migrate_response}})

            else:
                return jsonify({"flag": False, "message": migrate_response})

    except Exception as e:
        logging.info(f"####### Error in Validating or migration: {e}")

        return(jsonify({"flag": False, "message": "Could not validate or migrate cases"}))


@app.route('/dummy_case_creator', methods=['POST', 'GET'])
def create_addl_cases():
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    db_config['tenant_id'] = tenant_id
    host = os.environ.get('HOST_IP', "")
    port = "8080"

    task_id = 'demo'
    api_params = {}

    logging.debug(f'API Params: {api_params}')

    logging.debug(
        'Hitting URL: http://{host}:{8080}/rest/engine/default/process-definition/key/{task_id}/start')
    headers = {'Content-type': 'application/json; charset=utf-8'}
    requests.post(f'http://{host}:{port}/rest/engine/default/process-definition/key/{task_id}/start',
                  json=api_params, headers=headers)
    response_data = {'message': 'Successfully Created Case'}

    return jsonify({'flag': True, 'data': response_data})


@app.route('/moving_case_forcefully', methods=['POST', 'GET'])
def moving_case_forcefully():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.pop('tenant_id', None)
        db_config['tenant_id'] = tenant_id
        host = os.environ.get('HOST_IP', "")
        queue_db = DB('queues', **db_config)
        case_id = data.get('case_id', '')

        # Step1 Getting bpmn version ids
        logging.debug(
            f'Hitting URL: http://{host}:5002/get_bpmn_versions')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"tenant_id": tenant_id,
                      "bpmn_name": "adama_user_workflow"}  # Change the bpmn_name as per named in project
        bpmn_versions_response = requests.post(f'http://{host}:5002/get_bpmn_versions',
                                               json=api_params, headers=headers)
        logging.info(
            f"####### bpmn_versions_response Response: {bpmn_versions_response.text}")
        bpmn_versions_response = bpmn_versions_response.json()

        if len(bpmn_versions_response['data']['bpmns']) == 0:
            return {"flag": False, "message": "Could not find the bpmn version, Check the bpmn name in request"}

        # Step2 Getting processinstance id of the case
        bpmn_versions_response = bpmn_versions_response['data']['bpmns']
        vs = []
        for i in bpmn_versions_response:
            i = i.split(":")
            i[1] = int(i[1])
            vs.append(i[1])
        max_version = max(vs)
        latest_bpmn_version_index = vs.index(max_version)
        latest_bpmn_version = bpmn_versions_response[latest_bpmn_version_index]

        # bpmn_versions_response.sort()
        # latest_bpmn_version = bpmn_versions_response[-1]

        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"processDefinitionId": latest_bpmn_version}
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/task')
        logging.debug(f'api_params: {api_params}')
        instance_details = requests.post(f'http://{host}:8080/rest/engine/default/task',
                                         json=api_params, headers=headers)
        logging.info(
            f"####### instance_details Response: {instance_details.text}")
        instance_details = instance_details.json()
        logging.info(f'Instance details is {instance_details}')

        # Based on caseids(duplicates which are in customerdata) find the Taskid
        if case_id == '':
            return {"flag": False, "message": "Could not move forward case id to find task id is missing"}
        task_id_query = f"select * from queue_list where case_id='{case_id}' and queue='customer_data'"
        task_id_data = queue_db.execute_(task_id_query)
        task_id = task_id_data['task_id'][0]

        # Now search processInstanceId in the step2 response

        instance_details=pd.DataFrame(instance_details)
        instance_details=instance_details[(instance_details.name=='Farmer Data')]
        risk=instance_details[(instance_details.id==task_id)]
        risk.reset_index(inplace=True)
        processInstanceId_l=[]
        process_instance_id=risk['processInstanceId'][0]
        processInstanceId_l.append(process_instance_id)
        
        # Method 2 (The above method or below method both works)

        logging.info(
                        f"###### processInstanceId: {processInstanceId_l}")

        if not processInstanceId_l:
            return {"flag": False, "message": "Could not find the process instance id at stage 2"}
        else:
            processInstanceId = processInstanceId_l[0]

        # Step3 Finding activity instance id
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {}
        activity_instance_details = requests.get(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances',
                                                 json=api_params, headers=headers)
        logging.info(
            f"####### process_instance_details Response: {activity_instance_details.text}")
        activity_instance_details = activity_instance_details.json()

        # in this response collect the id from the childActivityInstances
        childActivityInstances = activity_instance_details['childActivityInstances']
        for i in childActivityInstances:
            for k, v in i.items():
                if 'customer_data:' in v:
                    childActivityInstance = v
                    logging.info(
                        f'###### childActivityInstance: {childActivityInstance}')

        # # Step4 modification api for case movement
        # if the response is 204 r success it is moved
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        
        # priority_queue value is hardcode (which queue will be there in queue_list table)
        api_params = {"instructions": [{"type": "startBeforeActivity", "activityId": "duplicate", "variables":{"priority_queue":{"value":"customer_data","local":True,"type":"String"}}}, {
            "type": "cancel", "activityInstanceId": childActivityInstance}], "annotation": "Modified to resolve an error."}
        modification_details = requests.post(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification',
                                             json=api_params, headers=headers)

        logging.info("####### Done with the modification!!")

        if modification_details.status_code == 204:
            return {'flag': True, 'data': {"message": "Successfully modified case in cockpit to the duplicate queue"}}
        else:
            return {'flag': False, 'data': {"message": "Failed in modifying case in cockpit to the duplicate queue"}}

    except Exception as e:
        logging.info(f"####### Error in moving the case: {e}")
        return {"flag": False, "message": "Could not move the case to previous stage"}



@app.route('/moving_case_forcefully_', methods=['POST', 'GET'])
def moving_case_forcefully_():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.pop('tenant_id', None)
        db_config['tenant_id'] = tenant_id
        host = os.environ.get('HOST_IP', "")
        queue_db = DB('queues', **db_config)
        case_id = data.get('case_id', '')

        # Step1 Getting bpmn version ids
        logging.debug(
            f'Hitting URL: http://{host}:5002/get_bpmn_versions')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"tenant_id": tenant_id,
                      "bpmn_name": "adama_user_workflow"}  # Change the bpmn_name as per named in project
        bpmn_versions_response = requests.post(f'http://{host}:5002/get_bpmn_versions',
                                               json=api_params, headers=headers)
        logging.info(
            f"####### bpmn_versions_response Response: {bpmn_versions_response.text}")
        bpmn_versions_response = bpmn_versions_response.json()

        if len(bpmn_versions_response['data']['bpmns']) == 0:
            return {"flag": False, "message": "Could not find the bpmn version, Check the bpmn name in request"}

        # Step2 Getting processinstance id of the case
        bpmn_versions_response = bpmn_versions_response['data']['bpmns']
        vs = []
        for i in bpmn_versions_response:
            i = i.split(":")
            i[1] = int(i[1])
            vs.append(i[1])
        max_version = max(vs)
        latest_bpmn_version_index = vs.index(max_version)
        latest_bpmn_version = bpmn_versions_response[latest_bpmn_version_index]

        # bpmn_versions_response.sort()
        # latest_bpmn_version = bpmn_versions_response[-1]

        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"processDefinitionId": latest_bpmn_version}
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/task')
        logging.debug(f'api_params: {api_params}')
        instance_details = requests.post(f'http://{host}:8080/rest/engine/default/task',
                                         json=api_params, headers=headers)
        logging.info(
            f"####### instance_details Response: {instance_details.text}")
        instance_details = instance_details.json()

        # Based on caseids(duplicates which are in customerdata) find the Taskid
        if case_id == '':
            return {"flag": False, "message": "Could not move forward case id to find task id is missing"}
        task_id_query = f"select * from queue_list where case_id='{case_id}' and queue='duplicate'"
        task_id_data = queue_db.execute_(task_id_query)
        task_id = task_id_data['task_id'][0]

        # Now search processInstanceId in the step2 response
        # if id found pick the processinstanceid in the same dict
        processInstanceId_l = []
        for i in instance_details:
            for k, v in i.items():
                if task_id == v:
                    processInstanceId = i['processInstanceId']
                    processInstanceId_l.append(processInstanceId)
                    logging.info(
                        f"###### processInstanceId: {processInstanceId}")

        if not processInstanceId_l:
            return {"flag": False, "message": "Could not find the process instance id at stage 2"}
        else:
            processInstanceId = processInstanceId_l[0]

        # Step3 Finding activity instance id
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {}
        activity_instance_details = requests.get(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances',
                                                 json=api_params, headers=headers)
        logging.info(
            f"####### process_instance_details Response: {activity_instance_details.text}")
        activity_instance_details = activity_instance_details.json()

        # in this response collect the id from the childActivityInstances
        childActivityInstances = activity_instance_details['childActivityInstances']
        for i in childActivityInstances:
            for k, v in i.items():
                if 'duplicate:' in v:
                    childActivityInstance = v
                    logging.info(
                        f'###### childActivityInstance: {childActivityInstance}')

        # # Step4 modification api for case movement
        # if the response is 204 r success it is moved
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        
        # priority_queue value is hardcode (which queue will be there in queue_list table)
        api_params = {"instructions": [{"type": "startBeforeActivity", "activityId": "reject", "variables":{"priority_queue":{"value":"duplicate","local":True,"type":"String"}}}, {
            "type": "cancel", "activityInstanceId": childActivityInstance}], "annotation": "Modified to resolve an error."}
        modification_details = requests.post(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification',
                                             json=api_params, headers=headers)

        logging.info("####### Done with the modification!!")

        if modification_details.status_code == 204:
            return {'flag': True, 'data': {"message": "Successfully modified case in cockpit to the reject queue"}}
        else:
            return {'flag': False, 'data': {"message": "Failed in modifying case in cockpit to the reject queue"}}

    except Exception as e:
        logging.info(f"####### Error in moving the case: {e}")
        return {"flag": False, "message": "Could not move the case to previous stage"}
