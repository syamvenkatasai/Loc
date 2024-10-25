"""
@author: Srinivasan Ravichandran
@modifier1: Gopi Teja B
@modifier2: Vyshnavi K
@modifier3: nikhil n
"""

import argparse
import json
import psutil
import ast
import requests
import os
import pandas as pd
import math
from app import app

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from flask import Flask, request, jsonify, url_for
from flask_cors import CORS
from db_utils import DB
from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
# from template_detection.template_detector import TemplateDetector
from time import time

from ace_logger import Logging
logging = Logging(name='detection_app')

# try:
#     with open('app/configs/template_detection_params.json') as f:
#         parameters = json.loads(f.read())
# except:
#     with open('configs/template_detection_params.json') as f:
#         parameters = json.loads(f.read())

# app = Flask(__name__)
# cors = CORS(app)


db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
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

def get_meta_data(case_id, tenant_id):
    """
    Author : Akshat Goyal

    :param case_id:
    :param tenant_id:
    :return:
    """
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)

    query = "select field from template_detection_config where type='meta'"
    meta_field_df = db.execute_(query)

    meta_field_cols = list(meta_field_df['field'])

    if not meta_field_cols:
        return []

    try:
        meta_field_cols_combined = ', '.join(meta_field_cols)
        query = "select %s from process_queue where case_id=%s"

        meta_data_df = db.execute_(
            query, params=[meta_field_cols_combined, case_id])
        meta_data = [list(meta_data_df[field])[0] for field in meta_field_cols]
    except:
        meta_data = []

    return meta_data


def get_ocr_data_pages(start_page, end_page, ocr_data):
    logging.info(f'-----------ocr_info pages are {start_page} ,{end_page}')
    return ocr_data[start_page:end_page+1]


def save_process_file(queue_db, case_id, document_id, single_doc_identifiers, tenant_id):
    query = f"SELECT `case_id` from `process_file` WHERE case_id='{case_id}' and document_id='{document_id}'"
    case_df = queue_db.execute_(query)
    linecoordinates = []
    properties = []
    lineandprop = []
    for i in single_doc_identifiers:
        linecoordinates_new_dict = {"SplittedScreenName": str(i['file_type']), "coordinates": [{"top": "0", "page": i['start_page'], "left":"0", "height":"0", "width":"0"}, {
            "top": "0", "page": i['end_page'], "left":"0", "height":"0", "width":"0"}], "SplittedFilePath": f"/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/{i['start_page']}_{i['end_page']}.pdf"}
        linecoordinates.append(linecoordinates_new_dict)

        # properties_new_dict = {str(i['start_page'])+'_'+str(i['end_page']) : {"templateName": i['filetype_template_name'],"templateType": i['file_type']}}
        properties_new_dict = {str(i['file_type']): {
            "templateType": i['file_type']}}
        properties.append(properties_new_dict)

        linecoordinates_new_dict['properties'] = properties_new_dict
        lineandprop_new_dict = linecoordinates_new_dict
        lineandprop.append(lineandprop_new_dict)
    query = (
        "insert into `process_file` (linecoordinates, properties, lineandprop, case_id, document_id) values (%s, %s, %s, %s, %s)"
        if case_df.empty
        else "update `process_file` set linecoordinates = %s, properties = %s, lineandprop = %s where case_id = %s and document_id=%s"
    )
    params = [json.dumps(linecoordinates), json.dumps(
        properties), json.dumps(lineandprop), case_id, document_id]
    queue_db.execute(query, params=params)
    return "updated data in process_file"


@app.route('/algonox_detect_template', methods=['POST', 'GET'])
@zipkin_span(service_name='detection_app', span_name='algonox_template_detection')
def algonox_template_detection_api():
    '''
    Sample of params:
    if doc splitter and want to find the file types available in the whole document
    {"tenant_id":"acepoc3","case_id":"ACEB1E9B4F","document_splitter":true,"file_type":"single_document"}

    if doc splitter and want to find the individual detection of each file types
    {"tenant_id":"acepoc3","case_id":"ACEB1E9B4F","document_splitter":true,"file_type":"individual_detection"}

    else (Normal file ingestiona and detection)
    {"tenant_id":"acepoc3","case_id":"ACEB1E9B4F"}

    '''
    data = request.json
    logging.info(f"Request Data of Template Detection: {data}")
    tenant_id = data.get('tenant_id', None)
    try:
        case_id= data['email']['case_id']
    except:
        case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    if (user is None) or (session_id is None):
        ui_data = data.get('ui_data', {'user':None,'session_id':None})
        user = ui_data['user']
        session_id = ui_data['session_id']
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='detection_app',
        span_name='algonox_template_detection_api',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
        try:
            doc_splitter = data.get('document_splitter', None)
            file_type = data.get('file_type', None)
            document_id_df = data.get('document_id_df', False)
            predicted_template=''
            # Initialization of DB
            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)
            if document_id_df!=True:
                if len(document_id_df) ==0:
                    try:
                        query = f"SELECT `document_id` from  `process_queue` where `case_id` = %s and state IS NULL"
                        document_id_df = queue_db.execute_(query, params=[case_id])['document_id'].tolist()
                    except:

                        return{'flag':False,'message':f'{case_id} is missing in the table'}
            else:
                document_id_df=[case_id]

            nothing_detected_all=0
            for document_id in document_id_df: 
                # Get OCR data of the file
                nothing_detected = False
                query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
                params = [case_id]
                ocr_info = queue_db.execute_(query, params=params)

                logging.info(f'####################### ocr_info')

                if ocr_info.empty:
                    ocr_data = {}
                    logging.warning('OCR data is not in DB or is empty.')
                else:
                    try:
                        ocr_data = json.loads(json.loads(list(ocr_info.ocr_data)[0]))
                    except:
                        ocr_data = json.loads(list(ocr_info.ocr_data)[0])

                if ocr_data:
                    logging.info(
                        f'## TD debug 01 class:`TemplateDetector` threshold = {parameters["matching_threshold"]}, address_threshold= {parameters["address_threshold"]}, file_type = {file_type}')

                    logging.info(
                        f'## TD debug 02 fn_name:`get_meta_data` input - case_id:{case_id}, tenant_id:{tenant_id}')
                    meta_data = get_meta_data(case_id=case_id, tenant_id=tenant_id)
                    logging.info(
                        f'## TD debug 02 fn_name:`get_meta_data` output - meta_data - {meta_data}')

                    if doc_splitter is not None and file_type == 'single_document':
                        td = TemplateDetector(tenant_id=tenant_id,
                                            threshold=parameters['matching_threshold'],
                                            address_threshold=parameters["address_threshold"],
                                            file_type=file_type
                                            )
                        logging.info(
                            f'## TD info doc_splitter is enabled {doc_splitter} calling single documents identification')
                        matched_templates = td.single_doc_identifier(ocr_data)
                        if len(matched_templates) > 0:
                            for i in range(len(matched_templates)):
                                try:
                                    next_start_page = matched_templates[i +
                                                                        1]['start_page']
                                    matched_templates[i]['end_page'] = next_start_page - 1
                                except IndexError:
                                    matched_templates[i]['end_page'] = int(
                                        (ocr_info.no_of_pages)[0]) - 1

                        logging.info(
                            f'## TD info doc_splitter is done result matched_templates - {matched_templates}')
                        query = "UPDATE `process_queue` SET `single_doc_identifiers`= %s WHERE `case_id` = %s and `document_id` = %s"
                        params = [json.dumps(matched_templates), case_id, document_id]

                        # save the splitting and template details in process_file table
                        # in order to load when case is open in UI splitter to show instead of empty fields
                        save_process_file(queue_db, case_id,
                                        document_id, matched_templates, tenant_id)

                        # stats_params = ['Detection', 'detection_app', 'process_queue',
                        #                 json.dumps({"state": f'Detected with filetype of large file {matched_templates}'}), f'{case_id}_{document_id}', 'case_id,template_name']
                        predicted_template = json.dumps(matched_templates)

                    elif doc_splitter is not None and file_type == 'individual_detection':
                        query = 'SELECT * FROM `process_queue` WHERE `case_id`=%s and `document_id`=%s'
                        params = [case_id, document_id]
                        process_queue = queue_db.execute_(query, params=params)

                        templates_found = (process_queue.single_doc_identifiers)[0]
                        if templates_found:
                            templates_found = ast.literal_eval(templates_found)
                            logging.info(
                            f'## TD info doc_splitter is done result templates_found - {templates_found}')
                            for count_, single_type in enumerate(templates_found):
                                
                                start_page = int(single_type['start_page'])
                                end_page = int(single_type['end_page'])
                                individual_file = single_type['file_type']
                                individual_file_type=''
                                for i in individual_file:
                                    if i=="_":
                                        break
                                    individual_file_type+=i
                                logging.info(
                                f'## TD info individual_file_type - {individual_file_type}')

                                td = TemplateDetector(tenant_id=tenant_id,
                                                    threshold=parameters['matching_threshold'],
                                                    address_threshold=parameters["address_threshold"],
                                                    file_type=file_type,
                                                    individual_file_type=individual_file_type
                                                    )

                                # Inorder to get the ocr data of specific pages
                                ocr_data_page_wise = get_ocr_data_pages(
                                    start_page, end_page, ocr_data)
                                logging.info(
                                f'## TD info ocr_Data_page_wise')

                                predicted_template, matched_templates = td.unique_fields(
                                    ocr_data_page_wise)
                                templates_found[count_]['template_name'] = matched_templates
                                logging.info(
                                f'## TD info templates_found[count_][template_name] matched_templates - {matched_templates}')

                        else:
                            templates_found = []
                        count = 0
                        for templates in templates_found:
                            if not templates['template_name']:
                                count += 1
                        if count == len(templates_found):
                            nothing_detected = True
                        logging.info(
                            f'## TD info doc_splitter is done result matched_templates - {templates_found}')
                        query = "UPDATE `process_queue` SET `single_doc_identifiers`= %s WHERE `case_id` = %s and `document_id`= %s"
                        params = [json.dumps(templates_found), case_id, document_id]
                        # stats_params = ['Detection', 'detection_app', 'process_queue',
                        #                 json.dumps({"state": f'Detected with individual file types found {templates_found}'}), f'{case_id}_{document_id}', 'case_id,template_name']
                        predicted_template = json.dumps(templates_found)

                    else:
                        template_name = ''
                        td = TemplateDetector(tenant_id=tenant_id,
                                            threshold=parameters['matching_threshold'],
                                            address_threshold=parameters["address_threshold"],
                                            file_type=file_type
                                            )

                        logging.info(
                            f'## TD debug 3 fn_name:`unique_fields` input - ocr_data - {ocr_data}, meta_data- {meta_data}, template_name-{template_name}')
                        predicted_template, matched_templates = td.unique_fields(
                            ocr_data, meta_data)
                        logging.info(
                            f'## TD debug 3 fn_name:`unique_fields` output - predicted_template - {predicted_template}, matched_templates- {matched_templates}')

                        template_prediction_record = {'unique': predicted_template}

                        logging.debug('Setting template name for the file in DB')
                        if predicted_template:
                            query = "UPDATE `process_queue` SET `template_name`= %s, `template_prediction_record` =%s WHERE `case_id` = %s"
                            params = [predicted_template, json.dumps(
                                template_prediction_record), case_id]
                            stats_params = ['Detection', 'detection_app', 'process_queue',
                                            json.dumps({"state": f'Detected with {predicted_template}'}), f'{case_id}_{document_id}', 'case_id,template_name']
                            queue_db.execute(query, params=params)
                        else:
                           pass
                else:
                    message = f'Template Detection Error: OCR data for case `{case_id}` not found.'
                    logging.error(message)
                    predicted_template = ''
                    # stats_params = ['Detection', 'detection_app', 'process_queue',
                    #                 json.dumps({"state": f'Detected with {predicted_template}'}), case_id, 'case_id,template_name']

                # Audit
                # stats_db = DB('stats', **db_config)
                # query = 'Insert into `audit` (`type`, `last_modified_by`, `table_name`, `changed_data`,`reference_value`,`reference_column`) values (%s,%s,%s,%s,%s,%s)'

                # stats_db.execute(query, params=stats_params)

                if nothing_detected:
                    query = "UPDATE `process_queue` SET  `detection_flag` =%s WHERE `case_id` = %s and `document_id` = %s"
                    params = [True, case_id, document_id]
                    nothing_detected_all+=1
                    queue_db.execute(query, params=params)
                           
            if not predicted_template:
                logging.info(f'files length is{document_id_df}  and nothing_detceted={nothing_detected_all}') 
                response_data = {'template_name': predicted_template, 'probability': -1,
                                    "detection_flag": False, "message": "no templates found for the file types"}
            else:
                response_data = {'template_name': predicted_template, 'probability': -1,
                                    "detection_flag": True, "message": "templates are found for the file types"}
            final_response_data =  {"flag": True, "data": response_data}

        except:
            message = "Something went wrong in detection service"
            response_data = {}
            logging.exception(message)
            final_response_data = {"flag":False, "message":message,"data":response_data}
        
    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / \
            (1024 * 1024 * 1024)
        end_time = time()
        memory_consumed = f"{memory_consumed:.10f}"
        time_consumed = str(round(end_time-start_time,3))
    except:
        logging.warning("Failed to calc end of ram and time")
        logging.exception("ram calc went wrong")
        memory_consumed = None
        time_consumed = None
        pass

    # insert audit
    audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id, 
                    "api_service": "algonox_detect_template", "service_container": "detetcion_api",
                    "changed_data": response_data,"tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(final_response_data['data']), "trace_id": trace_id,
                    "session_id": session_id,"status":str(final_response_data['flag'])}
    insert_into_audit(case_id, audit_data)

    return jsonify(final_response_data)


# detection two code starts here

def get_ocr(document_id):
    queues_db = DB('queues', **db_config)
    ocr_info_query = f"SELECT ocr_data FROM ocr_info where document_id='{document_id}'"
    ocr_info_df = queues_db.execute_(ocr_info_query)
    # ocr_data=ocr_info_df.ocr_data
    # ocr_data=template_info_df.to_dict()
    ocr_data = json.loads(ocr_info_df.ocr_data[0])
    all_pages = []
    for i in ocr_data:
        keywords = []

        for page in i:
            keywords.append(page['word'])

        statement = ' '.join(keywords)

        all_pages.append(statement)
    logging.info(f"Ocr data collected")
    return all_pages


def get_keywords_labels():
    """
    to get keywords and labels from database
    """
    try:
        template_db = DB('template_db', **db_config)
        keywords_query = "SELECT document_type, keywords, user_keywords FROM `detection_type`"
        keywords_info_df = template_db.execute_(
            keywords_query).to_dict(orient="records")
        logging.info(
            f"MTD fn name:`get_keywords_labels` query data:{keywords_info_df}")

        result_dict = {}

        keywords_list = []
        labels_list = []

        for item in keywords_info_df:
            document_type = item['document_type']
            keywords = ast.literal_eval(item['keywords'])
            user_keywords = ast.literal_eval(
                item['user_keywords']) if item['user_keywords'] is not None else None

            keywords=json.loads(keywords) #added in hdfc because datatype
            user_keywords=json.loads(user_keywords) #added in hdfc because datatype

            keyword_list = None if not keywords else [
                ', '.join([elem for sublist in keywords.values() for elem in sublist])]
            user_keyword_list = None if not user_keywords else [
                ', '.join([elem for sublist in user_keywords.values() for elem in sublist])]

            result_dict[document_type] = [keyword_list, user_keyword_list]

            if keyword_list is not None:
                keywords_list.extend(keyword_list)
                labels_list.append(document_type)

            if user_keyword_list is not None:
                keywords_list.extend(user_keyword_list)
                labels_list.append(document_type)

        logging.info(
            f"MTD fn_name:get_keywords_labels Keywords list received ")
        logging.info(
            f"MTD fn_name:get_keywords_labels labels list recieved  ")
        return keywords_list, labels_list
    except:
        logging.exception(
            "Something went wrong while fetching keywords and labels")
        return jsonify({'flag': False})


# Predict document labels
def predict_document_type(documents, keywords=[],labels=[],api=False):
    documents_predicted = {}
    page_no = 0
    
    # using this above keywords vector to calculate similarity
    for document in documents:
        # creating vector with keywords
        if not api:
            keywords, labels = get_keywords_labels()
            df = pd.DataFrame({'keywords': keywords, 'labels': labels})
            template_db = DB('template_db', **db_config)
            keywords_query = "SELECT max_thrushold,first_page_keywords,thrushhold  FROM `detection_type`"
            keywords_info_df = template_db.execute_(keywords_query).to_dict(orient="records")
            keywords_info_df = [item for item in keywords_info_df for _ in range(2)]
        else:
            df = pd.DataFrame({'keywords': keywords})
        vectorizer = TfidfVectorizer()
        page_no += 1
        temp = []
        vectorizer.fit(df['keywords'])
        tfidf_matrix = vectorizer.transform(df['keywords'])
        
        # Transform the new document using the trained vectorizer
        tfidf_vector = vectorizer.transform([document])

        # Compute similarity between the new document and all the training documents
        similarities = cosine_similarity(tfidf_vector,tfidf_matrix)[0]
            
        if api:
            max_similarities=max(similarities)
            return max_similarities
        
        logging.info(f"## TTD fun_name : {'thrushhold_compare'} input: (similarities,[document],labels,keywords_info_df) :{similarities,'document',labels,keywords_info_df}")
        predicted_label,max_similarity,Flag=thrushhold_compare(similarities,[document],labels,keywords,keywords_info_df)  
        logging.info(f"## TTD fun_name : {'thrushhold_compare'} output (predicted_label,max_similarity,Flag) :{predicted_label,max_similarity,Flag}")
        if not Flag:
            continue
        temp.append(max_similarity)
        temp.append(predicted_label)
        documents_predicted[page_no] = (temp)
    return documents_predicted


def thrushhold_compare(similarities,document,labels,keywords,keywords_info_df):
    """
    Author : Nikhil Naga N
    """
    df = pd.DataFrame({'keywords': keywords, 'labels': labels})
    logging.info(f"## TTD similarities got is {similarities}")
    logging.info(f"## TTD labels got is {labels}")
    logging.info(f"## TTD keywords_info_df got is {keywords_info_df}")
    # Find the index of the most similar document
    most_similar_index = similarities.argmax()
    
    max_similarity=max(similarities)
    logging.info(f"## TTD max_similarity got is {max_similarity}")
    if max_similarity<0.2:
        return '','',False
    
    # Retrieve the label of the most similar document
    predicted_label = df.loc[most_similar_index, 'labels']
    logging.info(f"## TTD predicted_label got is {predicted_label}")
    similarity_value=keywords_info_df[most_similar_index]['max_thrushold']
    logging.info(f"## TTD similarity_value got is {similarity_value}")

    if max_similarity > json.loads(similarity_value):
        logging.info(f"## TTD Entering IF condition ")
        #first page detection:
        meta=[keywords_info_df[most_similar_index]['first_page_keywords']]
        logging.info(f"## TTD meta got is {meta}")
        if not meta[0]:
            return predicted_label,max_similarity,True
        
        f_p_t=predict_document_type(document,meta,[],api=True)
        logging.info(f"## TTD f_t_p got is {f_p_t}")

        if f_p_t>json.loads(keywords_info_df[most_similar_index]['thrushhold']):
            return predicted_label,max_similarity,True
        else:
            new_similarities = np.delete(similarities, most_similar_index)
            # mask= similarities!=max_similarity
            # new_similarities=similarities[mask]
            del labels[most_similar_index]
            del keywords_info_df[most_similar_index] 
            del keywords[most_similar_index]
            return thrushhold_compare(new_similarities,document,labels,keywords,keywords_info_df)
    else:
        new_similarities = np.delete(similarities, most_similar_index)
        del labels[most_similar_index]
        del keywords_info_df[most_similar_index] 
        del keywords[most_similar_index]
        logging.info(f"## TTD Entering else condition")
        return thrushhold_compare(new_similarities,document,labels,keywords,keywords_info_df)
    
    
@app.route('/algonox_detect_type_document', methods=['POST', 'GET'])
@zipkin_span(service_name='detection_app', span_name='algonox_detect_type_document')
def algonox_detect_type_document():
    
    data = request.json
    logging.info(f"Request Data of Template Detection: {data}")
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    document_id = data.get('document_id', '')
    
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    if (user is None) or (session_id is None):
        ui_data = data.get('ui_data', {'user':None,'session_id':None})
        user = ui_data['user']
        session_id = ui_data['session_id']
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='detection_app',
        span_name='algonox_detect_type_document',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
    
        try:
            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)
            extraction_db = DB('extraction', **db_config)
            try:
                query = f"SELECT `document_id` from  `process_queue` where `case_id` = %s and state IS NULL"
                document_id_df = queue_db.execute_(query, params=[case_id])['document_id'].tolist()
                logging.info(f"document ids that we got is {document_id_df}")
            except:
                return {'flag':False,'message':f'{case_id} is missing in the table'}
            for document_id in document_id_df:    
                documents = get_ocr(document_id)
                predicted_label = predict_document_type(documents,[])

                indices = []
                for key in predicted_label.keys():
                    indices.append(key)
                result = {}
                for i in range(len(indices)):
                    start = indices[i]
                    if i < len(indices) - 1:
                        end = indices[i + 1] - 1
                    else:
                        end = len(documents)
                    result[(start, end)] = predicted_label[start][1]
                logging.info(f"document ids result is {result}")
                response = []
                for key, value in result.items():
                    temp = {}
                    start_page = key[0]
                    end_page = key[1]
                    individual_file_type = value
                    temp['start_page'] = start_page-1
                    temp['end_page'] = end_page-1
                    temp_1=value + ' '.join('_')+str(start_page)
                    temp['file_type'] = temp_1
                    response.append(temp)
                #response_data = {'detection_type': True}
                save_process_file(queue_db, case_id, document_id, response, tenant_id)
                if response:
                    logging.info(f"document ids output is {response}")
                    # update process_queue
                    query = "UPDATE `process_queue` SET  `single_doc_identifiers` =%s WHERE `case_id` = %s and `document_id` = %s"
                    params = [json.dumps(response), case_id, document_id]
                    queue_db.execute(query, params=params)
                    # logging.info(f"inserting type in ocr table for {document_id} type is {temp_1},{individual_file_type}")
                    type_ocr=f"UPDATE ocr SET SEG = '{value}' WHERE document_id = '{document_id}'"
                    type_ocr=extraction_db.execute_(type_ocr)

            # audit
            # stats_params = ['Detection', 'detection_type_document', 'process_queue',
            #                 json.dumps({"state": f'Detected with {response}'}), case_id, 'case_id,detection_type_data']
            # stats_db = DB('stats', **db_config)
            # query = 'Insert into `audit` (`type`, `last_modified_by`, `table_name`, `changed_data`,`reference_value`,`reference_column`) values (%s,%s,%s,%s,%s,%s)'

            # stats_db.execute(query, params=stats_params)
            final_response_data = {'flag': True, 'data': {'detection_type': True}}
        except Exception as e:
            message = f"Something went wrong in detection type service"
            response_data = {}
            logging.exception(message)
            final_response_data = {"flag":True, 'data': {'detection_type': True}}
        
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = time()
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id, 
                        "api_service": "algonox_detect_type_document", "service_container": "detetcion_api", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(final_response_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(final_response_data['flag'])}
        insert_into_audit(case_id, audit_data)

        return jsonify(final_response_data)



@app.route('/detection_data', methods=['POST', 'GET'])
def detection_data():
    """
    this function is to display the pagewise detected types on page aligner screen
    
    """
    data = request.json
    logging.info(f"Request Data for page detected types: {data}")
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
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
        service_name='detection_app',
        span_name='detection_data',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        try:
            db_config['tenant_id'] = tenant_id
            queues_db = DB('queues', **db_config)
            template_db=DB("template_db",**db_config)
            files_list=f"SELECT file_name FROM `process_queue` WHERE case_id='{case_id}' AND state is NULL"
            files_list=queues_db.execute_(files_list).values.tolist()
            files_list = [item for sublist in files_list for item in sublist]
            
            logging.info(f'files_list obtained is :{files_list}')

            query = "SELECT DISTINCT document_type FROM detection_type"
            query = template_db.execute_(query)
            query_data = query.values.tolist()
            result_dict = {item[0].lower().replace(" ", "_"): [] for item in query_data}
            output_dict = {}
            
            for each in files_list:
                file_name = each
                query_info = f"SELECT single_doc_identifiers FROM process_queue WHERE case_id='{case_id}' AND file_name='{each}'"
                query_info_df = queues_db.execute_(query_info)
                data_types = (query_info_df.to_dict())
                page_data = data_types['single_doc_identifiers'][0]
                page_data = json.loads(page_data)
                updated_data = [{"start_page": entry["start_page"],
                 "end_page": entry["end_page"],
                 "file_type": entry["file_type"].split('_')[0],
                 "template_name": entry["template_name"]} for entry in page_data]

                if page_data is None or query_data is None:
                    return_data = {'flag': False, 'message': 'data was not received properly'}
                else:
                    # Create a new file_dict for the current file_name
                    file_dict = {file_type: [] for file_type in result_dict.keys()}

                    for item in updated_data:
                        file_type = item['file_type'].lower().replace(" ", "_")
                        start_page = item['start_page']
                        end_page = item['end_page']
                        start_page=start_page+1
                        end_page=end_page+1
                        # Append the page range to the corresponding file_type in result_dict
                        if file_type in result_dict:
                            result_dict[file_type].extend(list(range(start_page, end_page + 1)))

                        if file_type in file_dict:
                            file_dict[file_type].extend(list(range(start_page, end_page + 1)))
                
                    output_dict[file_name] = file_dict
                
            return_data = output_dict
            logging.info(f"Response data: {return_data}")
            return_data = {'flag': True, 'data': return_data}
            
        except Exception as e:
            message = f"Something went wrong in finding the page wise types {e}"
            logging.exception(message)
            return_data = {'flag': False, 'message': message}

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = time()
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
        return return_data


@app.route('/get_type_identifiers', methods=['POST', 'GET'])
def get_type_identifiers():
    """
    to get type identifiers and display on UI Document type identifiers screen
    """
    data = request.json
    logging.info(f"Request Data for get_type_identifiers: {data}")
    
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
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
        service_name='detection_app',
        span_name='get_type_identifiers',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        try:
            doc_type = data.get('templateType', None)
            db_config['tenant_id'] = tenant_id
            template_db = DB("template_db", **db_config)
            
            if doc_type is None:
                return_data = {'flag': False, 'message': 'Template Type not received'}
            else:
                query_data = f"select `user_keywords` from `detection_type` where `document_type` = '{doc_type}'"
                query_data_df = template_db.execute_(query_data)
                query_data_df = query_data_df['user_keywords'][0]
                query_data_df = ast.literal_eval(query_data_df)
                query_info = query_data_df
                type_identifiers = query_info
                response_data = type_identifiers

                logging.info(f"TD fn_name :`get_type_identifiers` type wise identifiers recieved")
                return_data = {'flag': True, 'data': response_data}

        except Exception as e:
            message = f"Something went wrong in finding the type wise identifiers--{e}"
            logging.exception(message)
            return_data = {'flag': False, 'message': message}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = time()
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
        return return_data


@app.route('/reassigned_types', methods=['POST', 'GET'])  
def reassigned_types():
    """
      this function for updating data in process file and process queue after user manually reassign the file types
    """
    data = request.json
    data=data['ui_data']
    logging.info(f"Request Data for reassigned_types: {data}")
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    db_config['tenant_id'] = tenant_id
    queue_db = DB("queues", **db_config)

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
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
        service_name='detection_app',
        span_name='reassigned_types',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

    
        try:
            query =f"SELECT document_id,file_name FROM `process_queue` WHERE case_id='{case_id}' AND state is NULL"
            query=queue_db.execute_(query).values.tolist()
            
            files_info = {item[1]: item[0] for item in query}
            logging.info(f'files_information:::{files_info}')

            page_info=data['pageInfo']
            if len(page_info)==0 or page_info is None:
                return_data = {'flag': False, 'message': 'Page Information not received'}
            else:
                output_dict = {}
                for item in page_info:
                    for file_name, data in item.items():
                        file_data_list = []
                        for file_type, pages in data.items():
                            if pages:  # Check if pages list is non-empty
                                file_type_title_case = file_type.replace("_", " ").title()

                                # Group consecutive page numbers together
                                file_data = {"start_page": pages[0] - 1, "end_page": pages[0] - 1, "file_type": file_type_title_case}
                                for i in range(1, len(pages)):
                                    if pages[i] == pages[i - 1] + 1:
                                        file_data["end_page"] = pages[i] - 1
                                    else:
                                        file_data_list.append(file_data)
                                        file_data = {"start_page": pages[i] - 1, "end_page": pages[i] - 1, "file_type": file_type_title_case}
                                file_data_list.append(file_data)

                        output_dict[file_name] = sorted(file_data_list, key=lambda x: x['start_page'])
        
                for file_name in output_dict.keys():
                    document_id=files_info[file_name]
                    response=output_dict[file_name]
                    logging.info(f'document_id---{document_id}---{response}')
                    save_process_file(queue_db, case_id,document_id, response, tenant_id)
                    query = "UPDATE `process_queue` SET  `single_doc_identifiers` =%s WHERE `case_id` = %s and `document_id` = %s"
                    params = [json.dumps(response), case_id, document_id]
                    queue_db.execute(query, params=params)
                    logging.info(f"Reasssigned pages data: file name :{file_name}-document_id-{document_id}-{response}")
            
            message="Succesfully updated details "
            return_data = {'flag': True, 'data':{'message':message}}
            return return_data
                         
          
   
        except Exception as e:
            message = f"Something went wrong--{e}"
            logging.info(f"{e}")

            return_data = {'flag': False, 'message': message}
        
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = time()
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
        return return_data
        
@app.route('/get_template_names', methods=['POST', 'GET']) 
def get_template_names():
    """
    this function is to display available templates in UI for user
    """
    data = request.json
    logging.info(f"Request Data for page detected types: {data}")
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
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
        service_name='detection_app',
        span_name='get_type_identifiers',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        try:
            file_name = data['file_name']
            file_type = data['type']
            template_db = DB("template_db",**db_config)
            query = f"SELECT `template_name` FROM `trained_info` WHERE `file_name` = '{file_name}' AND `type`='{file_type}'"
            query = template_db.execute_(query).values.tolist()
            template_names = [item[0] for item in query]
            response_data = template_names
            logging.info(f"templates names are:{response_data}")
            return_data = {'flag': True, 'data': response_data}
        except Exception as e:
            message = f"Something went wrong in finding the template names--{e}"
            logging.exception(message)
            return_data = {'flag': False,'message':message}
        
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = time()
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
        return return_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int,
                        help='Port Number', default=5012)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')
    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, port=port, debug=False, threaded=True)
