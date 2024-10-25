import json
import argparse
import psutil
import os
import requests
import copy
from app import app
from db_utils import DB
import math
from difflib import SequenceMatcher
from collections import Counter
import statistics
import pandas as pd
import re
import joblib
import difflib
from time import time as tt
from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
from time import time

from app.utils import sort_ocr, get_pre_process_char_space, get_file_trace_train, check_multiple, validate, get_file_path, open_pdf, get_table_lines, case_in_file_trace
from app.string_matching import convert_ocrs_to_char_dict_only_al_num
from app.keyword_context_hierarchy.get_tables_utils import get_tables
from app.keyword_context_hierarchy.pattern_algo import get_value_ocr
from app.keyword_context_hierarchy.kv_with_hierarchial_context import extract_with_hierarchy
from app.extraction_1D_keyword import extraction_with_keyword, scope_extracrt

from time import time as tt


from ace_logger import Logging
logging = Logging(name="extraction_api")

with open('app/parameters.json') as f:
    parameters = json.loads(f.read())

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

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

def  get_file_data(case_id,document_id,api, queues_db,documnet_splitter):
    single_doc_identifiers=[]
    template_name=None
    process_file = queues_db.get_all('process_queue', condition={'document_id': document_id})
    if not process_file.empty:
        if documnet_splitter == False:
            template_name = process_file.template_name.values[0]
            if template_name is None or not template_name:
                message = 'Template name is none/empty string'
                logging.debug(f'## TE {message}')
                if not api:
                    return {'flag': False, 'message': message}
        else:
            single_doc_identifiers=process_file.single_doc_identifiers.values[0]
            print(f" ### TE func get_file_data output : single_doc_identifiers {single_doc_identifiers}")
        ocr_info = queues_db.get_all('ocr_info', condition={'document_id': document_id})
        
        try:
            ocr_data = json.loads(json.loads(list(ocr_info.ocr_data)[0]))
        except:
            ocr_data = json.loads(list(ocr_info.ocr_data)[0])

        if ocr_data is None or not ocr_data:
            message = 'OCR data is none/empty string'
            logging.debug(f'## TE {message}')
            return {'flag': False, 'message': message}
    else:
        message = f'document_id - {document_id} does not exist'
        return {'flag': False, 'message': message}

    return {'flag': True, 'ocr_data': ocr_data, 'template_name': template_name ,'single_doc_identifiers':single_doc_identifiers}



def get_field_data(api, result, template_name,single_doc_template='',splitter_flag=False):
    
    if api and not template_name:
        print(f' TE entering if with api : `{api}`')
        field_data = result['field_data']
        file_trace = result.get('file_trace', {})

        checkbox_data = result.get('checkbox_data', {})
        print(f' TE checkbox data we received `{checkbox_data}`')
    else:
        # * Get trained info of the template from the template_db
        print(f' TE entering else with api : `{api}`')

        templates_db = DB('template_db', **db_config)
        
        if splitter_flag:
            if template_name or template_name=="NULL":
                trained_info_query=f"SELECT * FROM `trained_info` WHERE `template_name` = '{single_doc_template}' and supported_template ='{template_name}'"
            else:
                trained_info_query=f"SELECT * FROM `trained_info` WHERE `template_name` = '{single_doc_template}'"

        else:
            trained_info_query = f"SELECT * FROM `trained_info` WHERE `template_name` = '{template_name}'"
        template_info_df = templates_db.execute_(trained_info_query)

        if template_info_df.empty:
            message = f'template - {template_name} does not exist'
            logging.debug(f'## TE {message}')
            return {'flag': False, 'message': message}

        # * Get fields to extract (fte) from the trained info
        field_data = json.loads(template_info_df.field_data.values[0])
        try:
            file_trace = json.loads(template_info_df.file_trace.values[0])
        except:
            file_trace = {}
        try:
            checkbox_data = json.loads(template_info_df.checkbox_data.values[0])
        except:
            pass

        remove_keys = ['header_ocr', 'footer_ocr', 'address_ocr']
        [field_data.pop(key, None) for key in remove_keys]  # Remove unnecessary keys
    return field_data, file_trace


def get_table_stuff(case_id,document_id, tenant_id):
    file_path = get_file_path(case_id,document_id, parameters['ui_folder'], tenant_id)

    print(f"## DEBUG 7.1 function name : get_file_path ,\n  input : {document_id} , {parameters['ui_folder']},tenant_id \n output : {file_path}")


    wand_pdf = open_pdf(file_path)

    print(f'## DEBUG 7.2 function name : open_pdf ,\n  input : {file_path} \n output : {wand_pdf}')

    tables_area = get_tables(file_path, document_id, tenant_id)
    print(f'## DEBUG 7.3 function name : get_tables ,\n  input : {file_path} \n {document_id} \n {tenant_id} \n output : {tables_area}')

    try:
        table_lines = get_table_lines(tables_area, wand_pdf)
        print(f'## DEBUG 7.4 function name : get_table_lines ,\n  input : {tables_area} \n {wand_pdf} \n output : {table_lines}')

    except:
        table_lines = None


    return file_path, wand_pdf, tables_area, table_lines

def get_match_threshold(false_positive, trained_info):
    match_ratio_thresholds = [1.0]

    threshold = trained_info.get('matching_threshold', 1.0)
    if not threshold in match_ratio_thresholds:
        match_ratio_thresholds.append(threshold)

    # changing this as by default we take false positive which is wrong
    if not false_positive:
        if not threshold in match_ratio_thresholds:
            match_ratio_thresholds.append(0.4)

    return match_ratio_thresholds


def prepare_extraction_data(fte_datas, file_trace, fte, pre_processed_char_space, ocr_data):
    intermediary_data = []
    for fte_data in fte_datas:
        # file_trace is not used any more - vahida
        # if "case_id" in fte_data and file_trace:
        #     if not case_in_file_trace(file_trace, fte_data['case_id'], fte):
        #         continue

        false_positive = fte_data.get('false_positive', False)
        # In trained_info matching_threshold is not present the output of this function is always 1.0 
        # match_ratio_thresholds = get_match_threshold(false_positive, fte_data)
        match_ratio_thresholds = [1.0]

        # print(f"## DEBUG 9.1 function_name : get_match_threshold , input : false_positive={false_positive} , fte_data={fte_data}  output : match_ratio_thresholds={match_ratio_thresholds}")

        

        strt_time=tt()
        ## value_ocr is same as ocr_data if pattern is not present in trained_info else, then value will be extracted from the pattern (Vahida) 

        pattern = fte_data.get('pattern', '')
        if pattern:
            value_ocr = get_value_ocr(pre_processed_char_space, fte_data, ocr_data)
        else:
            value_ocr = ocr_data
        end_time=tt()
        print(f" Time Consumed for get_value_ocr for {fte} is {str(end_time-strt_time)}")
        # print(f"## DEBUG 9.2 function_name : get_value_ocr , input : pre_processed_char_space={pre_processed_char_space} , fte_data={fte_data}, ocr_data={ocr_data}  output : value_ocr={value_ocr}")

        extend_thresholds = [1]

        for ratio in match_ratio_thresholds:
            for threshold in extend_thresholds:
                intermediary_data.append([fte_data, ratio, threshold, value_ocr])

    combined_data = sorted(intermediary_data, key=lambda x: (-x[1], x[2]))
    return combined_data

def value_only_field(fte_data, api):
    value_only = fte_data.get('value_only', False)
    val = ''
    method_used = ''
    word_highlight = {}
    if value_only and 'value' in fte_data:
        val = fte_data['value']
        method_used = 'Static'
        if api:
            word_highlight = {'top': 10000, 'bottom': 0, 'right': 0, 'left': 10000, 'word': fte_data['value'],
                              'page': -1}

    return val, word_highlight, method_used


def get_scope(fte_data):
    try:
        scope = json.loads(fte_data['scope'])
        if 'value_scope_actual' in fte_data and fte_data.get('value_scope_actual', None):
            scope_value = json.loads(fte_data['value_scope_actual'])
        else:
            scope_value = json.loads(fte_data['scope_value'])
    except:
        scope = fte_data['scope']
        if 'value_scope_actual' in fte_data and fte_data.get('value_scope_actual', None):
            scope_value = fte_data['value_scope_actual']
        else:
            scope_value = fte_data['scope_value']

    return scope, scope_value

def field_confidence_checker(val, word_highlight, field_conf_threshold):
    if 'confidence' in word_highlight:
        if word_highlight['confidence'] < field_conf_threshold:
            val = 'suspicious' + val
    else:
        logging.debug('no confidence field in highlight')

    return val

def extraction_func(fte, fte_data, fields_to_exclude, ocr_fields_dict, ocr_data, api, case_id, tenant_id,
               file_path, wand_pdf, tables_area, table_lines, pre_processed_char, ocr_length, match_ratio, value_ocr,
               extend_threshold):
    val = ''
    word_highlight = {}
    method_used = ''
    key_highlight = {}

    # fields_to_exclude is not present in the request data
    # if fields_to_exclude and fte in fields_to_exclude:
    #     return val, word_highlight, method_used, key_highlight

    try:
        fte = fte.strip()
    except:
        pass

    # * Get fields' confidence threshold
    if fte in ocr_fields_dict:
        field_conf_threshold = ocr_fields_dict[fte]['confidence_threshold']
    else:
        field_conf_threshold = 0.75

    logging.debug(f'Confidence for `{fte}` is {field_conf_threshold}')

    # if the field is static
    # Static fields are not used anymore so cmntd this - vahida
    # val, word_highlight, method_used = value_only_field(fte_data, api)
    # print(f'## DEBUG 10.1 function_name : value_only_field , input : fte_data={fte_data}, api={api}  output : val={val}, word_highlight={word_highlight}, method_used ={method_used}')
    # if val:
    #     return val, word_highlight, method_used, key_highlight
    
    
    scope, scope_value = get_scope(fte_data)
    print(f'## DEBUG 10.2 function_name : get_scope  output : scope={scope}, scope_value={scope_value}')
    keyword = fte_data['keyword'].encode('utf-8').decode()
    page_no = int(fte_data['page'])
    key_val_meta = fte_data.get('key_val_meta', None)
    context_key_field_info = fte_data.get('context_key_field_info', {})
    context_hierarchy = fte_data.get('context_hierarchy', {})

    pattern = fte_data.get('pattern', '')

    if context_hierarchy:
        
        print(f'Entering if condition function_name=extract_with_hierarchy')
        val, word_highlight, method_used, key_highlight = extract_with_hierarchy(fte, fte_data, ocr_data,
                                                                                 case_id, tenant_id,
                                                                                 file_path, wand_pdf,
                                                                                 tables_area, table_lines,
                                                                                 pre_processed_char,
                                                                                 field_conf_threshold,
                                                                                 match_ratio=match_ratio,
                                                                                 value_ocr=value_ocr,
                                                                                 extend_threshold=extend_threshold)
        print(f'## DEBUG 10.3 function_name : extract_with_hierarchy , input : fte={fte}, fte_data={fte_data}, case_id={case_id}, tenant_id={tenant_id},file_path={file_path}, wand_pdf={wand_pdf},field_conf_threshold={field_conf_threshold},match_ratio={match_ratio},value_ocr={value_ocr},extend_threshold={extend_threshold} output: val={val}, word_highlight={word_highlight}, method_used={method_used}, key_highlight={key_highlight}')
        if val:
            logging.debug('Context hierarchy was successful')
    # # FEUD
    # if not val and 'boundary_data' in fte_data and fte_data['boundary_data']:
    #     logging.debug(f"Trying to use FEUD")
    #     # print('in boundary data')
    #
    #     val, _ = find_field(fte, field_data, 'template_name', ocr_data)
    #     if val:
    #         output_['method_used'][fte] = 'FEUD'
    #         output_[fte] = val
    if not val:
        logging.debug("Unfortunately FUED was unsuccessful, But fear not keywords is here to rescue")

        # If keyword is there then get the nearest keyword to the trained
        # keyword and use relative position of that keyword to get the value
        logging.debug(f'keyword - {keyword}')
        logging.debug(f'scope - {scope}')
        if keyword and scope:
            key_page = int(page_no)
            try:
                val, word_highlight, method_used, key_highlight = extraction_with_keyword(
                    ocr_data, keyword, scope, fte, fte_data, key_page, ocr_length, context_key_field_info,
                    key_val_meta=key_val_meta, field_conf_threshold=field_conf_threshold,
                    pre_process_char=pre_processed_char, match_ratio=match_ratio, value_ocr=value_ocr,
                    extend_threshold=extend_threshold
                )
                print(f'## DEBUG 11 function_name: extraction_with_keyword output: val={val}, word_highlight={word_highlight}, method_used={method_used}, key_highlight={key_highlight}')
            except Exception as e:
                val = ''
                logging.exception(
                    'Error in extracting for field:{} keyword:{} due to {}'.format(fte, keyword, e))
        elif pattern:
            # we are already getting value_ocr using pattern
            for page in value_ocr:
                if page:
                    word_highlight = page[0]
                    val = word_highlight['word']
                    method_used = 'pattern'
        else:
            logging.debug('using coord method')
            # No keyword
            val, highlight = scope_extracrt(scope_value, scope, value_ocr, page_no, field_conf_threshold)
            print(f'## DEBUG 12 function_name scope_extracrt input: scope_value={scope_value}, scope={scope}, value_ocr={value_ocr}, '
                        f'page_no={page_no}, field_conf_threshold={field_conf_threshold} output: val={val}, highlight={highlight}')
            if val:
                word_highlight = highlight
                method_used = 'COORD'

    if isinstance(val, str):
        val = val.strip()

    val = field_confidence_checker(val, word_highlight, field_conf_threshold)
    print(f'## DEBUG 13 function_name field_confidence_checker input: word_highlight={word_highlight}, field_conf_threshold={field_conf_threshold} output: val={val}')

    return val, word_highlight, method_used, key_highlight

"""
def insert_into_audit(case_id, data):
    stats_db = DB('stats', **db_config)
    audit_data = {
        "type": "update", "last_modified_by": "Extraction", "table_name": "ocr",
        "reference_column": "case_id",
        "reference_value": case_id, "changed_data": json.dumps(data)
    }
    stats_db.insert_dict(audit_data, 'audit')
"""


def update_highlight_pages(word_highlights,key_highlight,start_page):
    for (k1,v1),(k2,v2) in zip(word_highlights.items(),key_highlight.items()):
        if v1['page'] < start_page:
            # ocr pages need to updated per the detected pages 
            v1['page']+= start_page
            v2['page']+=start_page
        else:
            pass
    return word_highlights, key_highlight

@zipkin_span(service_name='extraction_api', span_name='value_extract')
def value_extract(case_id,document_id,tenant_id,ocr_data,field_data, template_name, file_trace,fields_to_exclude,api=False,documnet_splitter=False,start_page=0):
    
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    template_db=DB('template_db', **db_config)
    # sort ocr messes up coordinates sometime
    
    pre_processed_char = []

    print(f'## TE info func_name : sort_ocr -- started')        
    print(f'## TE info func_name : convert_ocrs_to_char_dict_only_al_num -- started')

    for idx, page in enumerate(ocr_data):
        page = sort_ocr(page)
        ocr_data[idx] = page

        char_index_list, haystack = convert_ocrs_to_char_dict_only_al_num(page)
        pre_processed_char.append([char_index_list, haystack])
        
    print(f'## TE info func_name : sort_ocr -- completed')       
    print(f'## TE info func_name : convert_ocrs_to_char_dict_only_al_num -- completed')
    #use this logging statement when debug is required 
    # print(f'## TE info output is (pre_processed_char,ocr_data) : {pre_processed_char,ocr_data}')
    print(f'## TE info func_name : get_pre_process_char_space input (ocr_data)')       
    pre_processed_char_space = get_pre_process_char_space(ocr_data)
    print(f'## TE info func_name : get_pre_process_char_space output (pre_processed_char_space)')       

    # * Get the OCR fields
    #ocr_fields = queues_db.get_all('field_definition')
    #ocr_fields_list = ocr_fields.to_dict('records')
    ocr_fields_list = {}
    ocr_fields_dict = {}

    print(f'## TE info func_name : get_table_stuff input (case_id,document_id, tenant_id): {case_id,document_id, tenant_id}') 
    # get_table_stuff is not used anymore since the results are always empty commented - vahida      
    # file_path, wand_pdf, tables_area, table_lines = get_table_stuff(case_id,document_id, tenant_id)
    file_path=file_path = get_file_path(case_id,document_id, parameters['ui_folder'], tenant_id)
    wand_pdf=''
    tables_area=[]
    table_lines=[]
    print(f'## TE info func_name : get_table_stuff output (file_path, wand_pdf, tables_area, table_lines): {file_path, wand_pdf, tables_area, table_lines}')       

    for ocr_field_data in ocr_fields_list:
        ocr_fields_dict[ocr_field_data['unique_name']] = ocr_field_data
    
    json.dumps(field_data)

    ocr_length = len(ocr_data)
    word_highlights = {}
    key_highlight = {}
    output_ = {'method_used': {}}
    output_multiple = {}

    fields_trained = []
    file_trace={}
    # print(f'## TE info func_name : get_file_trace_train input (file_trace): {file_trace}')     
    ## file_trace is not used any more - vahida
    # try:    
    #     file_trace = get_file_trace_train(file_trace)
    # except Exception as e:
    #     file_trace={}
    # print(f'## TE info func_name : get_file_trace_train output (file_trace): {file_trace}')           

    print(f'## TE info Entering for loop for field_data : {field_data}')           
    for fte, fte_data_to_check in field_data.items():

        print(f'## TE info (fte,fte_data_to_check) for this iteration is : {fte , fte_data_to_check}')           

        if not fte_data_to_check:
            continue

        if isinstance(fte_data_to_check, dict):
            fte_datas = [fte_data_to_check]
        else:
            fte_datas = fte_data_to_check
        fields_trained.append(fte)
        
        print(f'## TE info func_name : prepare_extraction_data input (fte_datas, file_trace, fte, pre_processed_char_space, ocr_data)')           
        combined_data = prepare_extraction_data(fte_datas, file_trace, fte, pre_processed_char_space, ocr_data)
        print(f'## TE info func_name : prepare_extraction_data output (combined_data)')           

        print(f'## TE info Entering for loop for combined_data')           

        for fte_data, match_ratio, extend_threshold, value_ocr in combined_data:
            print(f'## TE info (fte_data, match_ratio, extend_threshold, value_ocr) for this iteration is : {fte_data, match_ratio, extend_threshold, value_ocr}')           
            val, word_highlight, method_used, key_high = (None,) * 4

            print(f'## TE info func_name : extraction_func input (fte, fte_data, fields_to_exclude,ocr_fields_dict,ocr_data, api, case_id, tenant_id,file_path, wand_pdf, tables_area,table_lines,pre_processed_char, ocr_length,match_ratio,value_ocr,extend_threshold)')        

            val, word_highlight, method_used, key_high = extraction_func(fte, fte_data, fields_to_exclude,
                                                                    ocr_fields_dict,
                                                                    ocr_data, api, case_id, tenant_id,
                                                                    file_path, wand_pdf, tables_area,
                                                                    table_lines,
                                                                    pre_processed_char, ocr_length,
                                                                    match_ratio=match_ratio,
                                                                    value_ocr=value_ocr,
                                                                    extend_threshold=extend_threshold)
            print(f'## TE info func_name : extraction_func output (val, word_highlight, method_used, key_high): {val, word_highlight, method_used, key_high}')        

            if val:
                word_highlight['word'] = val
                word_highlights[fte] = word_highlight
                output_[fte] = val
                output_['method_used'][fte] = method_used

                if key_high:
                    key_highlight[fte] = key_high

                # for multuple value
                #if fte_data.get('multiple_values', False) or ocr_fields_dict[fte].get('multiple_values', False):
                # if fte_data.get('multiple_values', False):
                #     logging.debug(f'## TE debug checking for multiple')
                #     field_multiple_name = fte + '_multiple'
                #     try:
                        
                #         print(f'## TE info func_name : check_multiple input (word_highlight, ocr_data): {word_highlight, ocr_data}')           
                #         output_multiple[field_multiple_name] = json.dumps(check_multiple(word_highlight, ocr_data[word_highlight['page']]))
                #         print(f'## TE info func_name : check_multiple output (output_multiple): {output_multiple}')           

                #     except:
                #         logging.exception(f'## TE exception error in finding multiple')
                break

    
    # try:
    #     word_highlights, output_ = checkbox_selector(case_id, checkbox_data, ocr_data, word_highlights, output, output_)
    # except Exception as e:
    #     pass

    # Find date related fields and change the format of the value to a standard one
    
    # standard_format = r'%Y-%m-%d'
    standard_format = r'%d-%m-%Y'
    print(f'## TE info func_name : validate output (output_, ocr_fields_dict, standard_format): {output_, ocr_fields_dict, standard_format}')           
    output_ = validate(output_, ocr_fields_dict, standard_format)
    print(f'## TE info func_name : validate output (output_): {output_}')    
    output_['method_used'] = json.dumps(output_['method_used'])
    save_data=True
    if api:
        save_data=False
        print(f'## TE info Its API fields')
        output_.pop('method_used', 0)
        word_highlights,key_highlight=update_highlight_pages(word_highlights,key_highlight,start_page)
        ## output_['word_highlights'] and flag are intialized to handle the audit data and same response
        output_['word_highlights']= word_highlights
        output_['highlight'] = [word_highlights, key_highlight]
        output_['flag']= True
        logging.debug(f" ### TE debug API is {api} and response data is {output_}")
        templates_db = DB('template_db', **db_config)
        query=f"select * from extracted_data_retrain where case_id='{case_id}' and template_name='{template_name}'"
        extracted_data_retrain_df=templates_db.execute_(query)
        if not extracted_data_retrain_df.empty:
            query=f"update extracted_data_retrain set extracted_data={output_} where case_id='{case_id}' and template_name='{template_name}'"
            templates_db.execute_(query)
        else:
            if template_name:
                data_to_store={'case_id':case_id,'template_name':template_name,'extracted_data':output_}

        return output_

    if save_data:
        word_highlights,key_highlight=update_highlight_pages(word_highlights,key_highlight,start_page)
        output_['word_highlights']= word_highlights
        output_['highlight'] = [word_highlights, key_highlight]
        print(f" ### TE info template_name got while saving is {template_name}")
        if template_name and output_['method_used']:
            data_to_store={'case_id':case_id,'template_name':template_name,'extracted_data':output_}
#         print(f" data to store for api is  {api} is {data_to_store}")
            template_db.insert_dict(data_to_store,'extracted_data_retrain')
        else:
            pass
            print(f" ### template name and output_ is not present for this template")


    
    # Update queue of the file. Field exceptions if there are any suspicious value else Verify queue.
    # queue_id_df = queues_db.get_all('workflow_stages', condition={'stage': 'detection'})
    # queue_id_df['id'] = queue_id_df.index
    # queue_id = list(queue_id_df['id'])[0]

    # query = 'SELECT * FROM `queue_definition` WHERE `default_flow`=%s'
    # move_to_queue_df = queues_db.execute(query, params=[queue_id])
    # if not move_to_queue_df.empty:
    #     move_to_queue = list(move_to_queue_df['unique_name'])[0]
    # else:
    #     move_to_queue = None

    # if os.environ['MODE'] == 'Test':
    #     output_.pop('method_used', 0)

    #     return_data = {
    #         'flag': True,
    #         'send_data': {
    #             'case_id': case_id,
    #             'template_name': template_name,
    #             'data': output_
    #         }
    #     }

    #     return return_data

    # updated_queue = move_to_queue

    # queues_db.update('process_queue', update={'queue': updated_queue}, where={'case_id': case_id}) # for CG specific

    output_['highlight'] = word_highlights
    output_['fields_trained'] = fields_trained
    # insert_into_audit(case_id, output_)
    # output_.pop('fields_trained')

    #logging.debug(f'Updated queue of case ID `{case_id}` to `{updated_queue}`')

    # serializing so that it can be stored
    output_['highlight'] = json.dumps(output_['highlight'])

    
    
    # todo try to not use this
    # query = "delete from `ocr` where `case_id`=%s"
    # extraction_db.execute_(query, params=[case_id])

    # extraction_db.insert_dict(table="ocr", data={'case_id': case_id})
    print(f'## TE info Updating ocr in ocr_table')

    for key, value in output_.items():
        try:
            if key=='highlight' or key=='fields_trained' or key=='word_highlights':
                continue         
            else:
                extraction_db.update("ocr", {key: value}, where={'document_id': document_id})
        except:
            message = f'Error inserting extracted data into the database.'
            logging.exception(f'## TE exception {message}')

    # logging.debug(f'output_multiple - {output_multiple}')
    # update multi value fields
    print(f'## TE info Updating ocr in ocr_table')
    for key, value in output_multiple.items():
        try:
            if key=='highlight':
                continue
            else:
                extraction_db.update("ocr", {key: value}, where={'document_id': document_id})
        except:
            message = f'Error inserting extracted field - {key} into the database.'
            logging.exception(f'## TE exception {message}')

    # try: query = f"Select id, communication_date_time, communication_date_time_bot from process_queue where case_id
    # = '{case_id}'" communication_date_bot = list(queues_db.execute(query).communication_date_time_bot)[0]
    # communication_date = list(queues_db.execute(query).communication_date_time)[0]
    #
    # query = f"update ocr set Fax_unique_id = '{case_id}', communication_date_time_bot = '{communication_date_bot}',
    # Communication_date_time = '{communication_date}' where case_id = '{case_id}' " extraction_db.execute(query)
    # except: pass


    return_data = {
        'flag': True,
        'send_data': {
            'case_id': case_id,
            'template_name': template_name,
            'output_data' : output_
        },
        "word_highlights":word_highlights
    }

    #no need to check document_splitter here
    return return_data


@app.route('/extraction', methods=['POST', 'GET'])
def extraction():
    data = request.json
    print(f'## TE info func_name : extraction_main \n input (data) : {data}')
    return(extraction_main(data))



def extraction_main(data,api=False):
    start_extrct_time=tt()
    print(f'## TE info request data: {data}')
    if data is None:
        return {'flag': False}
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
    try:
        tenant_id = data['tenant_id']
        try:
            case_id= data['email']['case_id']
        except:
            case_id = data.get('case_id', None)
        user = data.get('user', None)
        session_id = data.get('session_id', None)
    except Exception as e:
        logging.warning(f'## TE Received unknown data. [{data}] [{e}]')
        return {'flag': False, 'message': 'Incorrect Data in request'}
    
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
        service_name='extraction_api',
        span_name='extraction',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5, ):

        try:     

            documnet_splitter=data.get('document_splitter',False)
            static_data = data.get('static_data', {})
            # the request data not consist of fields_to_exclude 
            fields_to_exclude = static_data.get('fields_to_exclude', None)
            
            db_config['tenant_id'] = tenant_id
            queues_db = DB('queues', **db_config)
            extraction_db = DB('extraction', **db_config)

            ocr_data = data.get('ocr_data', [])
            template_name = data.get('template_name', None)
            document_id_ = data.get('document_id_df', [])
            if not document_id_:
                if len(document_id_df) ==0:
                    try:
                        query = f"SELECT `document_id` from  `process_queue` where `case_id` = %s and state IS NULL "
                        document_id_df = queues_db.execute_(query, params=[case_id])['document_id'].tolist()
                    except:
                        return{'flag':False,'message':f'{case_id} is missing in the table'}
            else:
                document_id_df=[case_id]
            
            
            print(f'## TE info document_ids that we recived : {document_id_df}')
            print(f'## TE info Entering for loop to extract for each documnet_id')

            for document_id in document_id_df:

                print(f'## TE info Extraction for documnet_id : {document_id}')

                print(f'## TE info func_name : get_file_data \n input (case_id,document_id, api, queues_db,documnet_splitter) : {case_id,document_id, api, queues_db,documnet_splitter}')
                file_data = get_file_data(case_id,document_id, api, queues_db,documnet_splitter)
                print(f"## TE info func_name : get_file_data \n output (file_data) : {file_data['template_name']}, {file_data['single_doc_identifiers']}")
                if file_data['flag']:
                    # if not api:
                    template_name = file_data['template_name']
                    single_template_data=file_data['single_doc_identifiers']    
                    ocr_data = file_data['ocr_data']
                else:
                    template_name = None
                    single_template_data=[]  
                    ocr_data = []
                    return file_data
            
                if documnet_splitter or documnet_splitter == "true":
                    print(f'## TE info entering if condition with documnet_splitter : {documnet_splitter}')
                    for values in json.loads(single_template_data):
                        single_template_name=values['template_name']
                        for i in single_template_name:
                            single_template=i
                            # template_name=i
                        if not single_template_name:
                            query = "UPDATE `process_queue` SET  `detection_flag` =%s WHERE `document_id` = %s"
                            params = [True, document_id]
                            queues_db.execute(query, params=params)
                            continue
                        start_page=values['start_page']
                        end_page=values['end_page']
                        temp_ocr_data=[]
                        for i in range(start_page,end_page+1):
                            temp_ocr_data.append(ocr_data[i])
                        
                        print(f'## TE info func_name : get_field_data \n input (api, data, template_name,single_template,documnet_splitter) : {api, data, template_name,single_template,documnet_splitter}')
                        field_data, file_trace = get_field_data(api, data, template_name,single_template,documnet_splitter)
                        print(f'## TE info func_name : get_file_data \n output (field_data, file_trace) : {field_data, file_trace}')

                        case_id = data['case_id']
                        #ocr_df = extraction_db.get_all('ocr')
                        file_name = data.get('file_name', '')

                        # if file_name=="":
                        #     return {'flag': False, 'message': 'Empty file_name'}

                        print(f'## TE info func_name : value_extract \n input ({case_id},{document_id},tenant_id,temp_ocr_data,field_data, {template_name}, file_trace,fields_to_exclude,{api},{documnet_splitter})')
                        response_data = value_extract(case_id,document_id,tenant_id,temp_ocr_data,field_data, single_template, file_trace,fields_to_exclude,api,documnet_splitter,start_page)
                        print(f'## TE info func_name : value_extract \n output (response_data) : {response_data}')

                        word_highlights = response_data['word_highlights']
                        # updating highlight pages 
                        # try:
                        #     for key,value in word_highlights.items():
                        #         if value['page'] < start_page or value['page'] > end_page:
                        #             #im just assuming and keeping the start page (vahida)
                        #             value['page']+= start_page
                        #         else:
                        #             pass
                        # except:
                        #     pass
                        query=f"select * from ocr where document_id='{document_id}'"
                        highlight_df=extraction_db.execute(query)
                        highlight=highlight_df.to_dict(orient='records')
                        highlight=highlight[0]['highlight']
                        highlight=json.loads(highlight)
        
                        if highlight:
                            highlight.update(word_highlights)
                            highlight=json.dumps(highlight)
                            query = "UPDATE `ocr` SET `highlight`= %s WHERE `document_id` = %s"
                            params = [highlight, document_id]
                            extraction_db.execute(query, params=params)
                        else:
                            word_highlight=json.dumps(word_highlights)
                            query = "UPDATE `ocr` SET `highlight`= %s WHERE `document_id` = %s"
                            params = [word_highlight, document_id]
                            extraction_db.execute(query, params=params)
                        extract_end_time=tt()
                        print(f" Time Consumed for entire Extraction for template {single_template} route is {str(extract_end_time-start_extrct_time)}")

                else:
                    print(f'## TE info entering else condition with documnet_splitter : {documnet_splitter}')

                    print(f'## TE info func_name : get_field_data \n input (api, data, template_name) : {api, data, template_name}')
                    field_data, file_trace = get_field_data(api, data, template_name)
                    print(f'## TE info func_name : get_field_data \n output (field_data, file_trace) : {field_data, file_trace}')

                    # case_id = data['case_id']
                    file_name = data.get('file_name', '')
                    documnet_splitter=False
                    # if file_name=="":
                    #     return {'flag': False, 'message': 'Empty file_name'}
                    start_page=0
                    print(f'## TE info func_name : value_extract \n input (case_id,document_id,tenant_id,ocr_data,field_data, template_name, file_trace,fields_to_exclude,api,documnet_splitter) ')
                    response_data = value_extract(case_id,document_id,tenant_id,ocr_data,field_data, template_name, file_trace,fields_to_exclude,api,documnet_splitter,start_page)
                    print(f'## TE info func_name : value_extract \n output (response_data) : {response_data}')


                    word_highlights = response_data['word_highlights']
                    query=f"select * from ocr where document_id='{document_id}'"
                    highlight_df=extraction_db.execute(query)
                    highlight=highlight_df.to_dict(orient='records')
                    highlight=highlight[0]['highlight']
                   
                    if highlight:
                        highlight=json.loads(highlight)
                        highlight.update(word_highlights)
                        highlight=json.dumps(highlight)
                        query = "UPDATE `ocr` SET `highlight`= %s WHERE `document_id` = %s"
                        params = [highlight, document_id]
                        extraction_db.execute(query, params=params)
                    else:
                        word_highlight=json.dumps(word_highlights)
                        query = "UPDATE `ocr` SET `highlight`= %s WHERE `document_id` = %s"
                        params = [word_highlight, document_id]
                        extraction_db.execute(query, params=params)
            
            # api become true , when the fields are extracted after re-training 
            if not api:
                changed_data_audit = {}
                if response_data['flag']:
                    changed_data_audit = response_data['send_data']['output_data']
                    response_data = {'flag': True, 'data': {'message':'Successfully extracted data'}}
                else:
                    logging.exception('## TE exception Errors in extraction')
                    response_data = {'flag': False, 'message': 'Errors in Extraction', 'data': response_data}
            else:
                changed_data_audit = {}
                # changed_data_audit = response_data['send_data']['output_data']
        except Exception as e:
            changed_data_audit = {}
            logging.exception(f'Error {e}')
            response_data = {'flag': False, 'message': 'Errors in Extraction'}
        
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
                        "api_service": "extraction", "service_container": "extraction_api", "changed_data": changed_data_audit,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(response_data), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(case_id, audit_data)
        print(f"## TE info func_name : extract_main \n final_output response_data is  : {response_data} \n api is {api}")

        if api:
            return response_data
        else:
            return jsonify(response_data)
            

@app.route('/predict_field', methods=['POST', 'GET'])
def predict_field():
    try:
        data = request.json
        # logging.debug(f"predict_field request data - {data}")
        response_data = extraction_main(data, api=True)
        logging.debug(f"predict_field response data - {response_data}")
        return jsonify(response_data)
    except Exception as e:
        logging.exception(f"Something went wrong while extracting. Check trace ........ {e}")
        return jsonify({'flag': False, 'message': 'System error! Please contact your system administrator.'})

@app.route("/check_extraction_api", methods=['POST', 'GET'])
def check_extraction_api():
    data = request.json
    print(f"Data recieved: {data}")
    try:
        print("checking extraction api status")
        return jsonify({"status":"True","message":"extraction api is UP and running."})

    except Exception as e:
        logging.debug("Something went wrong!")
        logging.exception(e)
        
    return jsonify({"status":"System error!"})


#------------------------------------------------------------------------------------------------


def predict_with_svm(word_list, div):
    """
    Predicts labels for words using Support Vector Machine (SVM) classifier.

    Args:
        word_list: List of words to be classified.
        div: Division or category for which the model is trained.

    Returns:
        field_keywords: Predicted labels for the words.
    """
    # File paths for the trained model and vectorizer
    model_filename = f"/var/www/extraction_api/app/extraction_folder/non_stand/{div}_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/non_stand/{div}_count_vectorizer.joblib"
    
    try:
        print(F"file found and file name is {vectorizer_filename},{model_filename}")
        # Load the trained model
        clf = joblib.load(model_filename)

        # Load the vectorizer used to convert words into numerical features
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(F"file not found and file name is {vectorizer_filename},{model_filename}")
        return {}
    try:
        # Convert the input list of words into a bag-of-words (BoW) representation
        X_inference = vectorizer.transform(word_list)

        # Make predictions using the loaded model
        predictions = clf.predict(X_inference)

        field_keywords = {}
        # Store predicted labels for each word
        for word, prediction in zip(word_list, predictions):
            if prediction != 'other':
                field_keywords[word] = prediction

        # Return the predicted labels
        return field_keywords
    except Exception as e:
        print(F"error is {e}")
        return {}


#------------------------------------------------------------------------------------------------


def second_head_check(finding):
    """
    Finds the accurate value if there are two values for the same key.

    Args:
        finding: List of words to be checked.

    Returns:
        bool: True if the word matches the expected header, False otherwise.
    """
    # Expected header for the second value
    word1 = 'amount'
    
    # List of words that indicate the second value should not be considered
    not_word_list = ['unit', 'number', 'unit rate']

    # Loop through each word in the list
    for word2 in finding:
        # Calculate similarity ratio between the word and the expected header
        similarity_ratio = difflib.SequenceMatcher(None, word1, word2['word'].lower()).ratio()
        
        # If similarity ratio is higher than 0.9, consider it as the second header
        if similarity_ratio > 0.9:
            return True
        else:
            # Check if the word is in the not_word_list
            for not_word in not_word_list:
                similarity_ratio_not = difflib.SequenceMatcher(None, not_word, word2['word'].lower()).ratio()
                # If similarity ratio is higher than 0.9, consider it as not a header
                if similarity_ratio_not > 0.9:
                    return False
            # If neither of the above conditions match, consider it as the second header
            return True


#------------------------------------------------------------------------------------------------


def find_table_header_non(each_value, ocr_data, header):
    """
    Finds the header for a given value found for a key.

    Args:
        each_value: Dictionary containing information about the value.
        ocr_data: List of dictionaries containing OCR data.
        header: List of header words.

    Returns:
        str: The header word if found, otherwise an empty string.
    """
    print(f"header got is {header}")
    return_list = []
    centroid = (each_value['left'] + each_value['right']) / 2
    head = []

    # Iterate through each word in the OCR data
    for word in ocr_data:
        if word['word'] in header:
            if (word['left'] <= centroid <= word['right'] or word['left'] <= each_value['right'] <= word['right'] or word['left'] <= each_value['left'] <= word['right']) and word['top'] < each_value['top']:
                return_list.append(word)
            elif (word['left'] <= each_value['left'] <= word['right']) and word['top'] < each_value['top']:
                return_list.append(word)
            elif (word['left'] <= each_value['right'] <= word['right']) and word['top'] < each_value['top']:
                return_list.append(word)
            # Condition to get header from the left if no header exists
            elif (word['right'] < each_value['left']) and word['top'] < each_value['top']:
                head.append(word)

    print(return_list, head)
    if return_list:
        nearest_dict = min(return_list, key=lambda x: abs(x['top'] - each_value['top']))
    else:
        if head:
            nearest_dict = min(head, key=lambda x: abs(x['right'] - each_value['left']))
            dif = abs(nearest_dict['right'] - each_value['left'])
            print(f"dif is {dif}")
            if dif > 40:
                print(F"retuing ''")
                return ''
        else:
            return ''
        
    if nearest_dict:
        finding = []
        for word in ocr_data:
            if nearest_dict['top'] < word['top'] and each_value['top'] > word['top']:
                if (word['left'] <= centroid <= word['right'] or word['left'] <= nearest_dict['right'] <= word['right'] or word['left'] <= nearest_dict['left'] <= word['right']) and word['top'] > nearest_dict['top']:
                    finding.append(word)
                elif (word['left'] <= nearest_dict['left'] <= word['right']) and word['top'] > nearest_dict['top']:
                    finding.append(word)
                elif (word['left'] <= nearest_dict['right'] <= word['right']) and word['top'] > nearest_dict['top']:
                    finding.append(word)
        
        if finding:
            out_res = second_head_check(finding)
            if out_res:
                return nearest_dict
            else:
                return ''
        else:
            return nearest_dict
    else:
        return ''

    
#------------------------------------------------------------------------------------------------


def find_table_second_header(each_value, ocr_data, header):
    """
    Finds the second header for a key that has no possible values in its line.

    Args:
        each_value: Dictionary containing information about the key.
        ocr_data: List of dictionaries containing OCR data.
        header: List of header words.

    Returns:
        str: The second header word if found, otherwise an empty string.
    """
    return_list = []
    centroid = (each_value['left'] + each_value['right']) / 2
    head = []

    # Iterate through each word in the OCR data
    for word in ocr_data:
        if word['word'] in header:
            if (word['left'] <= centroid <= word['right'] or word['left'] <= each_value['right'] <= word['right'] or word['left'] <= each_value['left'] <= word['right']) and word['top'] > each_value['top']:
                return_list.append(word)
            elif (word['left'] <= each_value['left'] <= word['right']) and word['top'] > each_value['top']:
                return_list.append(word)
            elif (word['left'] <= each_value['right'] <= word['right']) and word['top'] > each_value['top']:
                return_list.append(word)
            # Condition to get header from the left if no header exists
            elif word['top'] > each_value['top']:
                return_list.append(word)

    print(return_list, head)
    if return_list:
        out_re = []
        for x in return_list:
            dis = abs(x['top'] - each_value['top'])
            if dis <= 20 and x['left'] > each_value['left']:
                out_re.append(x)
        print(f"getting near based on top {out_re}")
        if out_re:
            nearest_dict = min(out_re, key=lambda x: abs(x['left'] - each_value['left']))
            print(f"nearest_dict is {nearest_dict} and each_value is {each_value}")
            if nearest_dict:
                return nearest_dict  
            else:
                return ''
        else:
            return ''
    else:
       return ''


#------------------------------------------------------------------------------------------------
    

def find_right_below_values(each_value, ocr_data, total='False'):
    """
    Finds the values that are present below the key for which we are predicting a value.

    Args:
        each_value: Dictionary containing information about the key.
        ocr_data: List of dictionaries containing OCR data.
        total: Optional parameter indicating whether to consider values above the key.

    Returns:
        list: List of dictionaries containing the values found below the key.
    """
    return_list = []
    centroid = (each_value['left'] + each_value['right']) / 2
    head = []

    # Iterate through each word in the OCR data
    for word in ocr_data:
        # Check if the word is below the key and within its horizontal boundaries
        if (word['left'] <= centroid <= word['right'] or word['left'] <= each_value['right'] <= word['right'] or word['left'] <= each_value['left'] <= word['right']) and word['top'] > each_value['top']:
            return_list.append(word)
        elif (word['left'] <= each_value['left'] <= word['right']) and word['top'] > each_value['top']:
            return_list.append(word)
        elif (word['left'] <= each_value['right'] <= word['right']) and word['top'] > each_value['top']:
            return_list.append(word)
        # Condition to include values above the key if total=True
        elif total and word['top'] < each_value['top']:
            return_list.append(word)
    
    # Return the list of values found below the key
    if return_list:
        return return_list
    else:
        return []


#---------------------------------------------------------------------------------------
    

def find_right_above_headers(each_value, ocr_data, keys_list, date):
    """
    Finds the header that is present above the value.

    Args:
        each_value: Dictionary containing information about the value.
        ocr_data: List of dictionaries containing OCR data.
        keys_list: List of headers.
        date: List of date headers.

    Returns:
        str: The header word if found, otherwise an empty string.
    """
    print(keys_list, each_value)
    return_list = []
    centroid = (each_value['left'] + each_value['right']) / 2
    head = []

    # Iterate through each word in the OCR data
    for word in ocr_data:
        # Check if the word is a header, not a date, and above the value
        if word['word'] in keys_list and word['word'] not in date:
            if (word['left'] <= centroid <= word['right'] or word['left'] <= each_value['right'] <= word['right'] or word['left'] <= each_value['left'] <= word['right']) and word['top'] <= each_value['top']:
                return_list.append(word)
            elif (word['left'] <= each_value['left'] <= word['right']) and word['top'] <= each_value['top']:
                return_list.append(word)
            elif (word['left'] <= each_value['right'] <= word['right']) and word['top'] <= each_value['top']:
                return_list.append(word)
            elif word['top'] <= each_value['top']:
                return_list.append(word)
            elif word['top'] - 10 <= each_value['top']:
                return_list.append(word)
    
    print(f"return_list in find_right_above_headers is {return_list}")

    # Find the nearest header above the value
    if return_list:
        nearest_dict = min(return_list, key=lambda x: abs(x['top'] - each_value['top']))
        print(nearest_dict)
        return nearest_dict['word']
    else:
        return ''


#---------------------------------------------------------------------------------------    

        
def line_wise_ocr_data(words):
    """
    Forms the values in each line and creates line-wise OCR data.

    Args:
        words: List of dictionaries containing OCR data.

    Returns:
        list: List of lists where each inner list represents words on the same horizontal line.
    """
    ocr_word = []

    # Sort words based on their 'top' coordinate
    sorted_words = sorted(words, key=lambda x: x["top"])

    # Group words on the same horizontal line
    line_groups = []
    current_line = []

    for word in sorted_words:
        if not current_line:
            # First word of the line
            current_line.append(word)
        else:
            diff = abs(word["top"] - current_line[-1]["top"])
            if diff < 5:
                # Word is on the same line as the previous word
                current_line.append(word)
            else:
                # Word is on a new line
                line_groups.append(current_line)
                current_line = [word]

    # Add the last line to the groups
    if current_line:
        line_groups.append(current_line)

    return line_groups


#---------------------------------------------------------------------------------------


def find_context(cont, ocr_data):
    """
    Finds the accurate keys using the provided context.

    Args:
        cont: List of context words.
        ocr_data: List of dictionaries containing OCR data.

    Returns:
        bool: True if any context word is found in the OCR data, False otherwise.
    """
    # Iterate through each word in the OCR data
    for data in ocr_data:
        word = data['word']
        # Iterate through each context word
        for value in cont:
            # Remove non-alphanumeric characters from the word for comparison
            word = re.sub(r'[^a-zA-Z0-9]', '', word)
            # Check if the context word is present in the current word
            if value.lower() in word.lower():
                print(f"word is {word}")
                return True
    # If no context word is found in the OCR data, return False
    return False


#---------------------------------------------------------------------------------------
  

def get_stock(page_stock, ocr_data, sto, stock_table_dicts, header, page_dict, level_2_keys_list,value_ke,gen_ke,Sec_head):
    """
    Extracts stock details from OCR data which has common keys.

    Args:
        page_stock: Current page number.
        ocr_data: List of dictionaries containing OCR data for the current page.
        sto: Stock details.
        stock_table_dicts: List of dictionaries containing information about stock table keys.
        header: Header information.
        page_dict: Dictionary containing page information.
        level_2_keys_list: List of level 2 keys.
        level_2_header: Header information for level 2.

    Returns:
        dict_wr: Extracted stock details.
    """

    # Finding the last key in the stock table
    stock_last_key = max(stock_table_dicts, key=lambda x: x["top"])
    print(f"  ################ stock_last_key is {stock_last_key}")

    return_list = []
    # Searching for 'Total' below the last key
    for word in ocr_data:
        for val_ke in value_ke:
            if val_ke in word['word'].lower() and word['top'] > stock_last_key['top']:
                return_list.append(word)
            break
    print(f"   ################  return_list in get_stock is {return_list}")

    if return_list:
        # Identifying nearest 'Total' to the last key
        nearest_dict = min(return_list, key=lambda x: abs(x['top'] - stock_last_key['top']))
        print(nearest_dict)
        # Finding next elements after the nearest 'Total' and filtering out numerical values
        next_values = find_next_ele(nearest_dict, ocr_data)
        numerical_values = filter_num(next_values)
        numerical_values = sorted(numerical_values, key=lambda x: x["right"])
        print(f"  ################ numerical_values in get_stock line {numerical_values} \n")
        # Extracting details from numerical values
        dict_wr, dict_wor,col_row_dict = extract_non_value_from_num(numerical_values, ocr_data, header, nearest_dict,
                                                        page_dict, level_2_keys_list, gen_ke,Sec_head)
        return dict_wr,dict_wor,col_row_dict
    else:
        return {},{},{}   


#---------------------------------------------------------------------------------------
    

def filter_num(numerical_values_temp,api=''):
    """
    Filters all the values found and retrieves the best possible values for extraction.

    Args:
        numerical_values_temp: List of dictionaries containing numerical values.

    Returns:
        list: List of dictionaries containing the filtered numerical values.
    """
    numerical_values = []

    # Iterate through each numerical value
    for each in numerical_values_temp:
        # Remove non-alphanumeric characters from the word
        alphanumeric_value = re.sub(r'[^a-zA-Z0-9]', '', each['word'])
        if api:
            alphanumeric_value = re.sub(r'[^0-9]', '', each['word'])
        
        # Check if the value is a digit and meets certain conditions
        if alphanumeric_value.isdigit():
            if api:
                numerical_values.append(each)
            elif (len(each['word']) > 2 or each['word'] == '0' or each['word'] == '-' or each['word'] == 'NIL' or each['word'] == 'nil' or each['word'] == 'NILL' or each['word'] == 'nill') and ("%" not in each['word'] and "/" not in each['word']):
                # print(f"Numerical value that is appending: {each['word']} and its length is {len(each['word'])}")
                numerical_values.append(each)
    
    # print(f"Numerical values obtained: {numerical_values}")

    return numerical_values


#---------------------------------------------------------------------------------------


def extract_non_value_from_num(numerical_values,ocr_data,header,data,page_dict,level_2_keys_list,predict,Sec_head):

    """
    Extracts the value from the numerical values found based on headers and context.

    Args:
        numerical_values: List of dictionaries containing numerical values.
        ocr_data: List of dictionaries containing OCR data.
        header: List of headers.
        data: Dictionary containing information about the key.
        page_dict: Dictionary containing page information.
        level_2_keys_list: List of level 2 keys.
        level_2_header: List of level 2 headers.
        predict: Predicted key.

    Returns:
        extracted value and its dictionary.
    """
    col_row_dict={}
    #In this function we will find possible value for the lable
    dict_temp={}
    dict_wor={}
    print(f" ################ key is {data}")
    #based on the number of possible values that we found for key we use the a way to extract the value

    #if only one value then we will extract value directly using this if condition logic
    if len(numerical_values)==1:
        print(F" ################ entered if")
        head=find_table_header_non(numerical_values[0],ocr_data,header)
        if head:
            if data['word'] not in dict_temp:
                dict_temp[data['word']]={}
            if data['word'] not in dict_wor:
                dict_wor[data['word']]={}
            if data['word'] not in col_row_dict:
                col_row_dict[data['word']]={}
            dict_temp[data['word']][head['word']]=numerical_values[0]['word']
            dict_wor[data['word']][head['word']]=numerical_values[0]
            col_row_dict[data['word']][head['word']]=[data,head]

    #if there are no values found then we will enter into this elif condition
    elif len(numerical_values)==0:
        print(F" ################ entered elif 1")
        #here we try to find if there is any second header like amount or value for the key
        second_head=find_table_second_header(data,ocr_data,Sec_head)
        print(f" ################ second_head got is {second_head}")
        if not second_head and predict =='DATE':
            second_head=data
        #if we found the second header then we will go with extarction uising the second header
        if second_head:
            print(F" ################ entered if sc")
            #we will find all possibel values for the second header 
            numerical_values_temp=find_right_below_values(second_head,ocr_data)
            numerical_values=[]
            numerical_values=filter_num(numerical_values_temp)
            print(f" ################ numerical_values got is {numerical_values}")
            #we will use those values and try to find the value
            ocr_data_line=line_wise_ocr_data(ocr_data)
            total_count=0
            for line in ocr_data_line:
                temp=0
                words_list = [d['word'] for d in line]
                if line[0]['top'] > second_head['top']:
                    print(f" ################ words_list is {words_list} for this line")
                    if predict != 'DATE':
                        for word in words_list:
                            if word in page_dict and word not in level_2_keys_list:
                                print(f" ################ breaking point 1")
                                temp=1
                                break
                    if temp==1 or total_count ==2:
                        break
                    for word in words_list:
                        for wo in numerical_values:
                            if word==wo['word']:
                                for word_ in line:
                                    if 'total' in word_['word'].lower():
                                        print(f" ################ total is present in {word_['word']}")
                                        total_count=total_count+1
                                        if data['word'] not in dict_temp:
                                            dict_temp[data['word']]={}
                                        if data['word'] not in dict_wor:
                                            dict_wor[data['word']]={}
                                        if data['word'] not in col_row_dict:
                                            col_row_dict[data['word']]={}
                                        dict_temp[data['word']][word_['word']]=word
                                        dict_wor[data['word']][word_['word']]=wo
                                        col_row_dict[data['word']][word_['word']]=[data,word_]

        #once we came across total keys then we will consider the value as the required value

        #if second head is not present then we will try to find the value by checking in the next possible lines             
        else:
            print(F" ################ entered else sc")
            ocr_data_line=line_wise_ocr_data(ocr_data)
            total_count=0
            for line in ocr_data_line:
                temp=0
                words_list = [d['word'] for d in line]
                words_list_ = [d for d in line]
                if line[0]['top'] > data['top']:
                    print(f" ################ words_list is {words_list} for this line")
                    #we will check for any headers are present in the next if yes then break
                    for word in words_list:
                        if word in page_dict and word not in level_2_keys_list:
                            print(" ################ breaking point 2")
                            temp=1
                            break
                    if temp==1 or total_count==2:
                        break
                    # if we didnt find any header in the next line then proced with extraction from next line
                    else:
                        numerical_values=[]
                        numerical_values=filter_num(line)
                        print(f" ################ numerical_values are {numerical_values}")
                        numerical_values = sorted(numerical_values, key=lambda x: x["right"])

                        #we will try to find the accurate value by using finding the header of the key
                        if numerical_values:
                            for value in numerical_values:
                                #if we find the header that is appropriate then we will consider it as the required field
                                head_=find_table_header_non(value,ocr_data,header)
                                print(f" ################ head is {head_} for value is {value} ")
                                if head_:
                                    head=head_['word']
                                    if data['word'] not in dict_temp:
                                        dict_temp[data['word']]={}
                                    if data['word'] not in dict_wor:
                                        dict_wor[data['word']]={}
                                    if data['word'] not in col_row_dict:
                                        col_row_dict[data['word']]={}
                                    dict_temp[data['word']][head]=value['word']
                                    dict_wor[data['word']][head]=value
                                    col_row_dict[data['word']][head]=[data,head_]
                                else:
                                    for word_ in words_list_:
                                        if 'total' in word_['word'].lower():
                                            total_count=total_count+1
                                            if data['word'] not in dict_temp:
                                                dict_temp[data['word']]={}
                                            if data['word'] not in dict_wor:
                                                dict_wor[data['word']]={}
                                            if data['word'] not in col_row_dict:
                                                col_row_dict[data['word']]={}
                                            dict_temp[data['word']][word_['word']]=value['word']
                                            dict_wor[data['word']][word_['word']]=value
                                            col_row_dict[data['word']][word_['word']]=[data,word_]

    #if we find multiple values for a key then we enter into this elif condition      
    elif len(numerical_values)>1:
        print(F" ################ entered elif 2")
        for value in numerical_values:
            #here we finalise the value based on the header that is found with respective to the value
            head_=find_table_header_non(value,ocr_data,header)
            print(f" ################ head is {head_} for value is {value} ")
            if head_:
                head=head_['word']
                if data['word'] not in dict_temp:
                    dict_temp[data['word']]={}
                if data['word'] not in dict_wor:
                    dict_wor[data['word']]={}
                if data['word'] not in col_row_dict:
                    col_row_dict[data['word']]={}
                dict_temp[data['word']][head]=value['word']
                dict_wor[data['word']][head]=value
                col_row_dict[data['word']][head]=[data,head_]

    print(f" ################ predicted for the key {dict_temp} and {dict_wor}")

    return dict_temp,dict_wor,col_row_dict


#------------------------------------------------------------------------------------------------


#searching for the values for the non standard keys in the entire ocr data 
def find_non_standard_values(keys_list,ocr_data_all,seg,header_all,predicted,page_dict,predict_page,level_2_keys_list,dates,high_dict,date_range_extract,general_key_varaibles,Sec_head):

    """
    Predicts non-standard values for each key based on the OCR data.

    Args:
        keys_list: Dictionary containing keys and their predicted labels.
        ocr_data_all: Dictionary containing OCR data for all pages.
        seg: Segment number.
        header_all: Dictionary containing headers for all pages.
        context: Context to identify accurate keys.
        predicted: Dictionary to store predicted values.
        page_dict: Dictionary containing page numbers for keys.
        predict_page: Dictionary to store page numbers for predicted values.
        level_2_keys_list: List of level 2 keys.
        level_2_header: List of level 2 headers.
        dates: Dictionary containing date information for pages.

    Returns:
        predicted values and their corresponding page numbers.
    """


    #we use a json file to get the fields that we need to extarct from each table
    json_file_path = f"/var/www/extraction_api/app/extraction_folder/non_stand/non_stand_db.json"
    corpus = read_corpus_from_json(json_file_path)[0]
    stock_head=[]
    for key,value in corpus.items():
        if value=='Stocks':
            stock_head.append(key)
    stock_table_page={}
    stock_table_dicts=[]

    column_row_dict={}

    #we start the prediction of each key for its lable
    for key,predict in keys_list.items():
            
            print(f" ################ ")
            print(f" ################ ")
            print(f" ################ ")
            print(F" ################ prediction starting for the key : {key}  and predict is {predict}\n")
            print(f" ################ ")
            print(f" ################ ")
            print(f" ################ ")

            page=page_dict[key]
            ocr_data=ocr_data_all[int(page)]
            header=header_all[page]
            date=dates[page]

            #we serach for the key that is predicted in ocr_data
            for data in ocr_data:
                word=data['word']
                if key.lower()==word.lower():
                    #once key is found then we will go for futher process
                    
                    #we will find all the possible value that are right next to the key
                    next_values=find_next_ele(data,ocr_data)
                    print(f" ################ right next values got is {next_values}")

                    #we we will filter all the values that are found right next to the key
                    numerical_values=[]
                    numerical_values=filter_num(next_values)
                    numerical_values = sorted(numerical_values, key=lambda x: x["right"])

                    print(f" ################ numerical_values after filtering are {numerical_values}")

                    #once values are found then send them to this function to find the acuarate values
                    print(f" ################ entering into extract_non_value_from_num")
                    dict_temp,dict_wor,col_row_dict=extract_non_value_from_num(numerical_values,ocr_data,header,data,page_dict,level_2_keys_list,predict,Sec_head)
                    print(f"  ################ output got from extract_non_value_from_num is {dict_temp}  and {dict_wor}")
                    print(f"  ################ col_row_dict is {col_row_dict}")

                    #once we get output value for the key we will store it in the predicted fields
                    if dict_temp:
                        if predict in stock_head:
                            stock_table_page[key]=page
                            stock_table_dicts.append(data)
                            
                        if predict == 'DATE':
                            for key__,head__ in dict_wor.items():
                                for ke,val in head__.items():
                                    head=find_right_above_headers(val,ocr_data,list(keys_list.keys()),date)
                                    print(f"  ################ head from find_right_above_headers is {head}")
                                    if head:
                                        he=keys_list[head]
                                        print(f"  ################ label for head is {he}")
                                        d_t=False
                                        for date_head in date_range_extract:
                                            if date_head in he.lower():
                                                rang=get_col_range(key__)
                                                print(f"  ################ rang for head is {rang}")
                                                if date_head+"_"+rang not in predicted:
                                                    predicted[date_head+"_"+rang]=[]
                                                if date_head+"_"+rang not in high_dict:
                                                    high_dict[date_head+"_"+rang]=[]
                                                if date_head+"_"+rang not in column_row_dict:
                                                    column_row_dict[date_head+"_"+rang]=[]
                                                predicted[date_head+"_"+rang].append(dict_temp)
                                                column_row_dict[date_head+"_"+rang].append(col_row_dict)
                                                predict_page[key]=page
                                                high_dict[date_head+"_"+rang].append(dict_wor)
                                                d_t=True
                                                break
                        else:
                            if predict not in predicted:
                                predicted[predict]=[]
                            if predict not in high_dict:
                                high_dict[predict]=[]
                            if predict not in column_row_dict:
                                column_row_dict[predict]=[]

                            predicted[predict].append(dict_temp)
                            column_row_dict[predict].append(col_row_dict)
                            predict_page[key]=page
                            high_dict[predict].append(dict_wor)  
    
    #getting common key values based on the table details predicted
    for gen_ke,value_ke in general_key_varaibles.items():
        if gen_ke not in predicted:
            sto=[]
            try:
                page_stock=statistics.mode(list(stock_table_page.values()))
            except:
                page_stock=''
            if page_stock:
                for ke,valu in stock_table_page.items():
                    if valu == page_stock:
                        sto.append(ke)
                print(f"  ################ page_stock is {page_stock} and {sto}")
                print(f"  ################ calling get_stock for common values extract")
                out_stock,dict_wo,col_row_dict=get_stock(page_stock,ocr_data_all[int(page_stock)],sto,stock_table_dicts,header_all[page_stock],page_dict,level_2_keys_list,value_ke,gen_ke,Sec_head)
                print(f"  ################ output got from get_stock is {dict_temp}  and {dict_wor}")
                print(f"  ################ col_row_dict is {col_row_dict}")
                if out_stock:
                    if gen_ke not in predicted:
                        predicted[gen_ke]=[]
                    if gen_ke not in high_dict:
                        high_dict[gen_ke]=[]
                    if gen_ke not in column_row_dict:
                        column_row_dict[gen_ke]=[]
                    predicted[gen_ke].append(out_stock)
                    high_dict[gen_ke].append(dict_wo)
                    column_row_dict[gen_ke].append(col_row_dict)
                    predict_page[list(out_stock.keys())[0]]=page_stock
        
    return predicted,predict_page,high_dict,column_row_dict


def is_alphanumeric(word):
    return any(char.isalpha() for char in word) and any(char.isdigit() for char in word)



#--------------------------------------------------------------------------------------


def generate_tab_view(data):
    print(F" ################# data got for table view fun is {data}")
    tabular_data = {
        "header": ["fieldName", "value", "aging", "margin", "formula"],
        "rowData": []
    }
    for field, value in data.items():
        if 'a.v' in value:
            value=list(value['a.v'].values())[0]
        else:
            value=value
        row = {"fieldName": field, "value": value, "aging": "", "margin": "", "formula": ""}
        tabular_data["rowData"].append(row)

    return tabular_data


def update_db_ntable(predicted_fields, case_id, tenant_id, seg='non_stand'):
    print(F"upadte into sdb fields got are {predicted_fields}")
    # Update database with extracted fields and their corresponding tables
    db_config['tenant_id'] = tenant_id
   
    # Connect to the extraction database
    extraction_db = DB('extraction', **db_config)
    print(F" ####### adding no of fileds extracted into field accuarcy table")

    # Read corpus from a JSON file
    json_file_path = f"/var/www/extraction_api/app/extraction_folder/non_stand/non_stand_db.json"
    corpus = read_corpus_from_json(json_file_path)[0]
    
    # Query to fetch required fields and their corresponding tables for the given segment
    query = f"select fields,table_div from required_fields_table where seg='{seg}'"
    req_fields = extraction_db.execute_(query).to_dict(orient="records")

    for table in req_fields:
        if table['fields']:
            fields=json.loads(table['fields'])
            tab=table['table_div']
        else:
            continue
        flat_list = [item for sublist in fields for item in sublist]
        for field in flat_list:
            corpus[field]=tab

    db_dict = {}
    # Update db_dict with predicted fields and their corresponding tables
    print(f"corpus is {corpus}")
    for key, value in predicted_fields.items():
        print(F"key is {key}")
        if key in corpus:
            table = corpus[key]
            if table not in db_dict:
                db_dict[table] = {}
            db_dict[table][key] = value

    print(F"db_dict before req {db_dict}")

    query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
    CUSTOMER_NAME = extraction_db.execute_(query)['customer_name'].to_list()[0]
    print(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
    unsubcribed_fields=[]
    try:
        if CUSTOMER_NAME:
            CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
            temaplate_db=DB('template_db',**db_config)
            query = f"SELECT `UNSUBCRIBED_FIELDS` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
            unsubcribed_fields_db = temaplate_db.execute_(query)['UNSUBCRIBED_FIELDS'].to_list()
            if unsubcribed_fields_db:
                unsubcribed_fields=json.loads(unsubcribed_fields_db[0])
            print(f"unsubcribed_fields got is {unsubcribed_fields}")
    except:
        pass

    print(F"req_fields before req {req_fields}")
    # Iterate through required fields and update db_dict
    for div in req_fields:
        div_tab = div["table_div"]
        req_fi = json.loads(div["fields"])
        if div_tab in db_dict:
            out_dic = db_dict[div_tab]
        else:
            db_dict[div_tab] = {}
            out_dic = db_dict[div_tab]
         
        print(req_fi,out_dic,db_dict)
       
        for re in req_fi:
            tem_fla = False
            for fie in re:
                if fie in out_dic:
                    if re[0] in out_dic:
                        if not out_dic[re[0]]:
                            out_dic[re[0]]=out_dic[fie]
                        if fie!=re[0]:
                            out_dic.pop(fie)
                        tem_fla = True
                    else:
                        out_dic[re[0]]=out_dic[fie]
                        if fie!=re[0]:
                            out_dic.pop(fie)
                        tem_fla = True

            if not tem_fla or re[0] in unsubcribed_fields:
                out_dic[re[0]] = ''
            
        print(out_dic)
        print(F"\n")
        db_dict[div_tab] = out_dic
    
    print(F"db_dict is {db_dict}")
    # Update OCR database with the extracted fields
    for key, value in db_dict.items():
        temp = key

        query = f"select `{temp}` from ocr where case_id='{case_id}'"
        try:
            column = extraction_db.execute_(query)[temp].to_list()[0]
        except:
            column = extraction_db.execute_(query)[temp.lower()].to_list()[0]
        if column:
            column=json.loads(column)
            for key,value_ in value.items():
                column[key]=value_

            value=column

        value['tab_view']=generate_tab_view(value)

        chunk_size = 4000 
        value=json.dumps(value)
        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {temp} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            sql += "TO_CLOB('" + chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"

        # query = f"UPDATE `ocr` SET `{temp}` = '{json.dumps(value)}' WHERE `case_id` = '{case_id}'"
        extraction_db.execute_(sql)
    
    return True


#---------------------------------------------------------------------------------------


def from_high_cor(words):
    try:
        temp = {}
        temp['word'] = words['word']
        temp['top'] = words['top']
        temp['left'] = words['left']
        temp['bottom'] = words['bottom']
        temp['right'] = words['right']
        temp['page'] = words['pg_no']-1
        temp['x'] = words['left']
        temp['y'] = words['top']

        max_height = temp['bottom'] - temp['top']
        total_width = temp['right'] - temp['left']
        temp['height'] = max_height
        temp['width'] = total_width

        return  temp
    except:
        return {}



def get_highlight_n_table(update_ocr_data,predict_page,ocr_data_word_all,seg):
    try:
        new_page_predict={}
        for key__,value__ in predict_page.items():
            new_page_predict[key__.lower()]=value__
        value_highlights = {}

        for key, values in update_ocr_data.items():
            temp_1={}
            for pred in values:
                for col,head_dict in pred.items():
                    for head,value_dict in head_dict.items():
                        try:
                            start_page = int(new_page_predict[col.lower()])
                        except:
                            start_page = int(new_page_predict[head.lower()])
                        ocr_data_word = ocr_data_word_all[start_page]
                        for words in ocr_data_word:
                            temp = {}
                            if compare_dicts(words, value_dict):
                                temp=from_high_cor(words)
                                break
                        value_highlights[col+'_'+head]=temp   
        print(f" ################ value_highlights is {value_highlights}")
            
        return value_highlights
    except Exception as e:
        print(f" ################ error occurred in fn get_highlight_n_table. Error is {e}")
        return {}
    

 
def compare_dicts(dict1, dict2):
    if set(dict1.keys()) != set(dict2.keys()):
        return False
    for key in dict1:
        if dict1[key] != dict2[key]:
            return False
    return True


def get_context(update_ocr_data,predict_page,column_head_dic,ocr_data_word_all,seg):

    new_page_predict={}
    for key__,value__ in predict_page.items():
        new_page_predict[key__.lower()]=value__
    predict={}
    for key, values in update_ocr_data.items():
            pred_temp={}
            for index,pred in enumerate(values):
                for col,head_dict in pred.items():
                    for head,value_dict in head_dict.items():
                        try:
                            print(f"predict_page is {predict_page,col}")
                            start_page = int(new_page_predict[col.lower()])
                        except:
                            start_page = int(new_page_predict[head.lower()])
                        ocr_data_word = ocr_data_word_all[start_page]
                        sorted_words = sorted(ocr_data_word, key=lambda x: x["top"], reverse=True)
                        col_f=column_head_dic[key][index][col][head][0]
                        row_top=column_head_dic[key][index][col][head][1]['top']
                        cont=get_cont(row_top,sorted_words,1000,col_f)
                        print(f"cont is {cont}")
                        if not cont:
                            cont=get_cont(row_top,sorted_words,1000,col_f,True)
                        print(F"final cont is {cont} ")
                        
                        pred_temp[col+'_'+head]=cont
                           
  
            predict[key] = pred_temp

    return predict



def get_training_data(case_id,tenant_id):

    db_config['tenant_id']=tenant_id
    template_db=DB('template_db',**db_config)
    extraction=DB('extraction',**db_config)
    query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
    CUSTOMER_NAME = extraction.execute_(query)['customer_name'].to_list()[0]
    print(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
    PRIORITY_DATA={}
    PRIORITY_CONTEXT={}
    PRIORITY_FORMULA={}
    USER_TRAINED_FIELDS={}
    TRAINED_DATE=''
    date=''
    if CUSTOMER_NAME:
        CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
        query = f"SELECT `PRIORITY_DATA`,`PRIORITY_CONTEXT`,`PRIORITY_FORMULA`,`USER_TRAINED_FIELDS`,`TRAINED_DATE` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
        data = template_db.execute_(query)
        print(f"CUSTOMER_NAME got is {data}")
        try:
            PRIORITY_DATA=data['PRIORITY_DATA'].to_list()
            if PRIORITY_DATA:
                PRIORITY_DATA=json.loads(PRIORITY_DATA[0])
            else:
                PRIORITY_DATA={}
        except:
            PRIORITY_DATA={}
        try:
            PRIORITY_CONTEXT=data['PRIORITY_CONTEXT'].to_list()
            if PRIORITY_CONTEXT:
                PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT[0])
            else:
                PRIORITY_CONTEXT={}
        except:
            PRIORITY_CONTEXT={}
        try:
            PRIORITY_FORMULA=data['PRIORITY_FORMULA'].to_list()
            if PRIORITY_FORMULA:
                PRIORITY_FORMULA=json.loads(PRIORITY_FORMULA[0])
            else:
                PRIORITY_FORMULA={}
        except:
            PRIORITY_FORMULA={}
        try:
            USER_TRAINED_FIELDS=data['USER_TRAINED_FIELDS'].to_list()
            if USER_TRAINED_FIELDS:
                USER_TRAINED_FIELDS=json.loads(USER_TRAINED_FIELDS[0])
            else:
                USER_TRAINED_FIELDS={}
        except Exception as e:
            print(F" ######## {e}")
            USER_TRAINED_FIELDS={}
        try:
            TRAINED_DATE=data['TRAINED_DATE'].to_list()
            if TRAINED_DATE:
                TRAINED_DATE=json.loads(TRAINED_DATE[0])
            else:
                TRAINED_DATE=''
        except Exception as e:
            print(F" ######## {e}")
            TRAINED_DATE=''
        
    return PRIORITY_CONTEXT,PRIORITY_DATA,PRIORITY_FORMULA,USER_TRAINED_FIELDS,TRAINED_DATE



def get_cont(row_top,sorted_words,width,col_f,api=''):
    cont={}
    print(F"cont is {cont} and width is {width} and row_top is {row_top} and {col_f}")
    if width<100:
        return cont
    
    for word in sorted_words:
        if not api:
            if word['top']<row_top:
                if int(abs(word['right']-word['left']))>width:
                    print(F"here is {word} and {col_f}")
                    word['hypth']=calculate_distance(word, col_f)
                    word['context_width']=width
                    word['position']='above'
                    cont=word
                    break
        else:
            if word['top']>row_top:
                if int(abs(word['right']-word['left']))>width:
                    print(F"here is {word} and {col_f}")
                    word['hypth']=calculate_distance(word, col_f)
                    word['context_width']=width
                    word['position']='below'
                    cont=word
                    break
    if not cont:
        return get_cont(row_top,sorted_words,width-100,col_f,api)
    else:
        return cont





def using_context(col,cont,values,ocr_data_all,predict_page,api):
    try:
        for value in values:
            page=value['pg_no']
            ocr_data=ocr_data_all[int(page)-1]
            for data in ocr_data:
                word=data['word']
                word=re.sub(r'[^a-zA-Z0-9]', '', word)
                if api:
                    if word.lower() in cont['word'].lower():
                        return value['word']
                else:
                    for co in cont:
                        if word.lower() in co.lower():
                            return value['word']
    except:
        pass
    return values[0]['word']



def find_next_ele_crop(key,ocr_data,thr):
    
    excceded_min_flag=False
    next_values=[]
    sorted_words = sorted(ocr_data, key=lambda x: x["top"])
    k_right=key["right"]
    min=100
    sorted_words=line_wise_ocr_data(ocr_data)
    for line in sorted_words:
            if key in line:
                line.remove(key) 
            if line:
                diff_ver=abs(abs((line[0]["top"]+line[0]["bottom"])/2) - abs((key["top"]+key["bottom"])/2))
                dif=abs(diff_ver-thr)
                if min>dif:
                    print(F" here diff for values are {min  , dif}")
                    next_values=[]
                    min=dif
                    for word in line:
                        if k_right<word['left']:
                            next_values.append(word)
    print(f"  we have for next values as {next_values} where thr is {thr}")
    if min>5:
        excceded_min_flag=True

    return next_values,excceded_min_flag,min



def find_table_crop_header(each_value,ocr_data,header,col_f):

    print(f" ################ header got is {header}")
    return_list=[]
    centorid=(each_value['left']+each_value['right'])/2
    head=[]
    sorted_words=line_wise_ocr_data(ocr_data)
    for line in sorted_words:
            flag=False
            for word in line:
                if (word['left']<=centorid<=word['right'] or word['left']<=each_value['right']<=word['right'] or word['left']<=each_value['left']<=word['right'] )and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=each_value['left']<=word['right'] ) and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=each_value['right']<=word['right'] ) and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=each_value['left']-10<=word['right'] ) and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=each_value['left']-20<=word['right'] ) and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=each_value['left']-30<=word['right'] ) and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=each_value['left']-40<=word['right'] ) and word['top'] < each_value['top']:
                    return_list.append(word)
                    flag=True
            if flag == False:
                sorted_left = sorted(line, key=lambda x: x["left"],reverse=True)
                for wo in sorted_left:
                    if wo['right']<each_value['left'] and abs(wo['right']-each_value['left'])<100 and wo['top'] < each_value['top']:
                        if return_list and return_list[-1]['bottom']<wo['top']:
                            return_list.append(wo)
                            break
                        else:
                            return_list.append(wo)
                            break


    sorted_list_head = sorted(return_list, key=lambda x: x['top'],reverse=True)
    print(f" ################ sorted_list_head got is {sorted_list_head}")
    row_head_l=[]
    row_head=''

    for item in sorted_list_head:
        # print(F" item is {item}")
        i_cen=abs(item['right']+item['left']/2)
        t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
        t_alp=re.sub(r'[^a-zA-Z]', '', item['word'])
        numeric_headers=['90','0','180','91','120','121','150','151','181','270','271','365']
        if not t:
            continue
        if not row_head_l:
            row_top=item['top']
        if t and len(t)>=4 and t.isalpha():
            # print(F" item is {item}")
            if abs(item['bottom']-row_top)<=(2*abs(item['top']-item['bottom'])):
                # print(abs(item['bottom']-row_top),abs(item['top']-item['bottom']))
                if col_f['right']<item['left']:
                    row_head_l.append(item['word'])
                    row_top=item['top']       
        else:
            numeric=re.sub(r'[^a-zA-Z0-9]', ' ', item['word'])
            list_i=numeric.split()
            t_fl=False
            for i in list_i:
                i_numeric=re.sub(r'[^a-zA-Z]', '', i)
                if i not in numeric_headers and not i_numeric:
                    t_fl=True
                    break
            if not t_fl:
                if abs(item['bottom']-row_top)<=(2*abs(item['top']-item['bottom'])):
                    if col_f['right']<item['left']:
                        row_head_l.append(item['word'])
                        row_top=item['top']
        # print(F"row_head_l is {row_head_l}")

    for head in row_head_l:
        row_head=row_head+' '+head
    
    hea=re.sub(r'[^a-zA-Z]', '', header)
    row_h=re.sub(r'[^a-zA-Z]', '', row_head)
    print(F" ############ hea and row_h are {hea,row_h}")
    matcher = SequenceMatcher(None, row_h, hea)
    similarity_ratio_head = matcher.ratio()
    print(F' ################ final row head got is {row_head} and similarity_ratio_head is {similarity_ratio_head}')
    return row_head,similarity_ratio_head
    

def get_col(col,cont,ocr_data_all,distance,flag=True):

    out_col=[]
    max_col=0.5
    max_con=0.5
    temp_col_max=0
    max_match_column_dict={}
    temp_con_max=0
    max_match_con_dict={}
    col_=re.sub(r'[^a-zA-Z]', '', col)
    cont_=re.sub(r'[^a-zA-Z]', '', cont['word'])
    for ocr_data in ocr_data_all:
        tem_col=[]
        con_dict_poss=[]
        col_dict_poss=[]
        for word in ocr_data:
            wo=re.sub(r'[^a-zA-Z]', '', word['word'])
            if col_:
                matcher = SequenceMatcher(None, col_, wo)
                similarity_ratio_col = matcher.ratio()
                if similarity_ratio_col>max_col:
                    if similarity_ratio_col>temp_col_max:
                        temp_col_max=similarity_ratio_col
                        max_match_column_dict=word
                    col_dict_poss.append(word)
            if cont_:
                matcher = SequenceMatcher(None, cont_, wo)
                similarity_ratio_con = matcher.ratio()
                if similarity_ratio_con>max_con:
                    if similarity_ratio_con>temp_con_max:
                        temp_con_max=similarity_ratio_con
                        max_match_con_dict=word
                    con_dict_poss.append(word)
        tem=1000
        print(F" ################ con_dict_poss amd col_dict_poss is {con_dict_poss} and {col_dict_poss}")
        if not col_dict_poss and max_match_column_dict:
            col_dict_poss.append(max_match_column_dict)
        if not con_dict_poss and max_match_con_dict:
            con_dict_poss.append(max_match_con_dict)
        print(F" ################ con_dict_poss amd col_dict_poss is {con_dict_poss} and {col_dict_poss}")
        if con_dict_poss and col_dict_poss:
            for item1 in col_dict_poss:
                for item2 in con_dict_poss:
                    dista = calculate_distance(item1, item2)
                    print(F"dista is {dista} and distance is {distance}")
                    if abs(distance-dista)<tem:
                        tem=abs(distance-dista)
                        tem_col=[item1,tem,item2]
        if tem_col:
            out_col.append(tem_col)

    print(f" ################ column possibilites got are {out_col}")

    if out_col:
        sorted_list = sorted(out_col, key=lambda x: x[1])
        column_dict=sorted_list[0][0]
        cont_dict=sorted_list[0][2]
    else:
        column_dict=max_match_column_dict
        cont_dict={}
    print(f" ################ finalised column is {column_dict}")
    if flag:
        return column_dict
    else:
        return column_dict,cont_dict


def get_crop_values(col,head,cont,ocr_data_all):

    print(f" ################ \n")
    print(f" ################ \n")
    print(f" ################ \n")
    print(f" ################ col head cont send to get_crop_values are {col,head,cont}")
    print(f" ################ \n")
    print(f" ################ \n")
    print(f" ################ \n")

    distance=cont.get('hypth',0)
    # position=cont['position']
    if '@' in col:
        diff_col=col.split('@')[1]
    else:
        diff_col=''
    
    column_dict=get_col(col,cont,ocr_data_all,distance)
    excceded_min_flag=False
    if diff_col:
        next_values,excceded_min_flag,min_diff=find_next_ele_crop(column_dict,ocr_data_all[column_dict['pg_no']-1],float(diff_col))
    else:
        next_values,excceded_min_flag,min_diff=find_next_ele_crop(column_dict,ocr_data_all[column_dict['pg_no']-1],float(0))
        if min_diff>10:
            next_values=[]
                    
    print(f" ################ next_values got is {next_values}")

    #we we will filter all the values that are found right next to the key
    numerical_values=[]
    if next_values:
        # numerical_values=filter_num(next_values,True)
        numerical_values = sorted(next_values, key=lambda x: x["right"])
    
    print(f"################ numerical_values after filtering {numerical_values}")
    out_val={}
    thr=0
    if head:
        for value in numerical_values:
            alphanumeric_value = re.sub(r'[^a-zA-Z0-9]', '', value['word'])
            if not alphanumeric_value:
                continue
            #here we finalise the value based on the header that is found with respective to the value
            head_,similarity_ratio_head=find_table_crop_header(value,ocr_data_all[value['pg_no']-1],head,column_dict)
            print(f" ################ head is {head} for value is {value}")
            if head_ and similarity_ratio_head>thr:
                out_val=value
                thr=similarity_ratio_head

        print(f" ################ value got is {out_val} ")
        if out_val:
            return out_val
        else:
            return numerical_values[0]
    
    if not head and (not numerical_values or excceded_min_flag):
        print(F" ############## no head and no numbeers or excedded flag {excceded_min_flag} for  {column_dict}")
        temp_word = column_dict['word']
        column_dict['word']=re.sub(r'[^0-9,.-]', '', column_dict['word'])
        return {temp_word:[column_dict]},{}
    temp={}
    other={}
    for i,num in enumerate(numerical_values):
        if i==0:
            temp[column_dict['word']+str(num['word'])]=[num]
        else:
            other[column_dict['word']+str(num['word'])]=num['word']
    return temp,other
    

def calculate_distance(point1, point2):
    # print(point1,point2)
    x1, y1 = point1['left'], point1['top']
    x2, y2 = point2['left'], point2['top']
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return distance


def get_opt_frm(trained_frmls,found_vals):

    frmla=''
    final_all_parts=[]
    temp_key=list(found_vals.keys())
    for frml in trained_frmls:
        parts = []
        all_parts=[]
        current_part = ''
        for char in frml:
            if char in ['+', '-', '*', '/']: 
                parts.append(current_part)
                all_parts.append(current_part)
                all_parts.append(char)
                current_part = ''
            else:
                current_part += char
        parts.append(current_part)
        all_parts.append(current_part)
        if temp_key==parts:
            final_all_parts=all_parts
            frmla=frml
            break

    if frmla:
        num_frml=[found_vals.get(part, part) for part in final_all_parts]
        return frmla,eval(num_frml)
    else:
        return '',''


def priority_func(seg,predicted,case_id,tenant_id,ocr_data_all,predict_page,rec_flag=''):
    
    prio_context,prio_data,prio_formula,user_trained_fields,date=get_training_data(case_id,tenant_id)
    print(f" ################ prio_data got is  {prio_data}")
    print(f" ################ prio_context is {prio_context}")
    print(f" ################ prio_context is {prio_formula}")
    print(f" ################ user_trained_fields is {user_trained_fields}")

    ocr_data_all_copy=copy.deepcopy(ocr_data_all)

    if prio_data:
        flag=False
    else:
        flag=True

    json_file_path = f"/var/www/extraction_api/app/extraction_folder/non_stand/extract_variables.json"
    corpus = read_corpus_from_json(json_file_path)
    #env variables that can be changed based on the requriements:
    default_context=corpus['default_context']
    head_prio=corpus['head_prio']

    croped_high={}
    print(F' ################ starting priority function')
    final_pairs={}
    out_col=[]
    label_col={}
    pri_col_head={}
    crop_fields={}
    
    for key in prio_data:
        if key not in predicted:
            predicted[key]=[{}]

    for label, values in predicted.items():
        print(F" ################ label is {label}")
        temp_col=[]
        for pred in values:
            for col,head_dict in pred.items():
                temp_col.append(col)
        if label in prio_data and not rec_flag:
            print(F" ################ label present in prio_data")
            for temp_ou,val_ou in prio_data[label].items():

                if '_crop' in temp_ou or values == [{}]:
                    if label not in crop_fields:
                        crop_fields[label]=[]
                    print(F" ################ label is field cropped")
                    ou=temp_ou
                    crop_fields[label].append(ou.split('_')[0]+"~"+val_ou)

                elif temp_ou in temp_col:
                    print(F" ################ column is present in predicted cols")
                    ou=temp_ou
                    pri_col_head[ou]=val_ou
                else:
                    print(F" ################ not a croped or found in the predicted cols")
                    if temp_col:
                        ou=find_values_n(seg,temp_col,label)
                if not ou:
                    ou=temp_col[0]
                out_col.append(ou)
                label_col[ou]=label
        else:
            ou=find_values_n(seg,temp_col,label)
            if not ou:
                ou=temp_col[0]
            out_col.append(ou)
            label_col[ou]=label
    print(f" ################ out_col are column finalised are {out_col}")
    temp_head_all={} 
    head_val={}
    for label, values in predicted.items():
        for pred in values:
            for col,head_dict in pred.items():
                for co in out_col:
                    if col == co:
                        if co not in temp_head_all:
                            temp_head_all[co]=[]
                        for head,value in head_dict.items():
                            temp_head_all[co].append(head)
                            head_val[head]=value

    print(f" ################ headers for each col is temp_head_all:{temp_head_all} ")
    print(F" ################ headers to values mapped dict is head_val:{head_val}")
    for co,temp_head in temp_head_all.items():
        if co in pri_col_head:
            temp_h=pri_col_head[co]
            if temp_h in temp_head:
                out_head=temp_h
            else:
                temp=0
                out_head=''
                for pri in head_prio:
                    if temp==1:
                        break
                    for he in head:
                        if pri in he.lower():
                            out_head=he
                            temp=1
                            break
                if not out_head:
                    out_head=find_values_n(seg,temp_head,'RH','head')
        else:
            temp=0
            out_head=''
            for pri in head_prio:
                if temp==1:
                    break
                for he in head:
                    if pri in he.lower():
                        out_head=he
                        temp=1
                        break
            if not out_head:
                out_head=find_values_n(seg,temp_head,'RH','head')
        final_pairs[label_col[co]]={co:out_head}
    print(f" ################ final_pairs for col and header are {final_pairs}")
    print(f" ################ final_pairs for crop_fields are {crop_fields}")
    print(f" ################ predicted are {predicted}")
    final_pred={}
    training={}
    for label, values in predicted.items():
        temp={}
        other={}
        rec_pred={}
        if label not in final_pred:
            final_pred[label]={}

        if label in crop_fields:
            if label not in rec_pred:
                rec_pred[label]=[]
            all_label_vals={}
            te={}
            for crop_field in crop_fields[label]:
                ocr_data_all=copy.deepcopy(ocr_data_all_copy)
                print(F" ################ label is in cropped fileds {label}")
                te_col, te_head = crop_field.split('~', 1)
                try:
                    mini_flag=False
                    if te_head:
                        try:
                            crop_value=get_crop_values(te_col,te_head,prio_context[label][te_col+te_head],ocr_data_all)
                            print(f" ################ crop_value is {crop_value} for te_head {te_head} and te_col {te_col}")
                            if crop_value:
                                te_col=te_col.split('@')[0]
                                if te_col not in te:
                                    te[te_col]={}
                                te[te_col][te_head]=crop_value
                                all_label_vals[te_col+'_'+te_head]=crop_value
                                croped_high[te_col+'_'+te_head]=from_high_cor(crop_value)
                        except Exception as e:
                            mini_flag=True
                    elif not te_head or mini_flag:
                        print(F" ################ Non header")
                        temp,other=get_crop_values(te_col,te_head,prio_context[label][te_col+te_head],ocr_data_all)
                        if temp:
                            crop_temp=list(temp.values())[0]
                            if from_high_cor(crop_temp):
                                croped_high[list(temp.keys())[0]]=from_high_cor(crop_temp)
                            else:
                                croped_high[list(temp.keys())[0]]=from_high_cor(crop_temp[0])
                            for other_hed,other_val in other.items():
                                croped_high[other_hed]=from_high_cor(other_val)
                        print(F"for non header head an dvalues git are {crop_temp} and {croped_high}")
                except Exception as e:
                    print(f" ##################### there is some error here with {crop_field} and error is {e}")
                    continue
            if not temp:
                print(f" ################ te for rec is {te}")
                rec_pred[label].append(te)
                print(f" ################ rec_pred for rec is {rec_pred}")
                if label in prio_formula:
                    trained_frmls=prio_formula[label]
                else:
                    trained_frmls=[]
                print(f" ################ trained_frmls is {trained_frmls}")
                if trained_frmls:
                    opt_frml,val=get_opt_frm(trained_frmls,all_label_vals)
                    if val:
                        temp[te_col+'_'+te_head]=[opt_frml]
                        print(f" ################ trained_frmls output is  {val} and formula is {opt_frml}")
                    else:
                        temp,other=priority_func(seg,rec_pred,case_id,tenant_id,ocr_data_all,predict_page,True)
                        print(f" ################ trained_frmls failed so called rec outs are {other} and {temp}")
                elif len(all_label_vals) >= 1:
                    print(f" ################ all_label_vals is {all_label_vals}")
                    temp[list(all_label_vals.keys())[0]]=[list(all_label_vals.values())[0]]
                # else:
                #     # try:
                #     #     temp,other=priority_func(seg,rec_pred,case_id,tenant_id,ocr_data_all,predict_page,True)
                #     #     print(f" ################ no trained_frmls so called rec outs are {other} and {temp}")
                #     # except:
                #     continue

        if label in final_pairs:
            for pred in values:
                for col,head_dict in pred.items():
                    for head,value in head_dict.items():
                        print(F" ################ label is in non cropped fileds {label}")
                        if col in final_pairs[label] and head==final_pairs[label][col]:
                            if col+"_"+head not in temp:
                                temp[col+"_"+head]=[value]
                            else:
                                temp[col+"_"+head].append(value)
                        else:
                            other[col+"_"+head]=value['word']
        rec_temp=temp
        for key,values in temp.items():
            if len(values)>1:
                if label in prio_context and key in prio_context[label]:
                    context=prio_context[label][key]
                    api=True
                else:
                    context=default_context
                    api=False
                temp[key]=using_context(key,context,values,ocr_data_all,predict_page,api)
            else:
                try:
                    temp[key]=values[0]['word']
                except:
                    temp[key]=''

        if temp:            
            final_pred[label]['a.v']=temp
            final_pred[label]['r.v']=other
            if label in final_pairs:
                training[label]=final_pairs[label]
            
    if not rec_flag:
        return final_pred,training,croped_high,flag,user_trained_fields
    else:
        return rec_temp,final_pred[label]['r.v']

#------------------------------------------------------------------------------------------------



def find_values_n(seg,word_list,desired_label,col='col'):

    # print(f" recived list and lable for find_values is {word_list} -------------> {desired_label}")
    
    model_filename = f"/var/www/extraction_api/app/extraction_folder/non_stand/non_stand_{col}_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/non_stand/non_stand_{col}_count_vectorizer.joblib"
    try:
        # Load the model
        loaded_model = joblib.load(model_filename)

        # Load the vectorizer
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(f"######################### joblib file for this seg is missing {vectorizer_filename} and {model_filename}")
        return {}
    word_list_lower = [word.lower() for word in word_list]

    # Preprocess the list of words using the loaded vectorizer
    word_list_vectorized = vectorizer.transform(word_list_lower)

    # Get the probability estimates for the classes
    proba_estimates = loaded_model.predict_proba(word_list_vectorized)

    model_classes = loaded_model.classes_

    # Check if the desired label is in the model's classes
    if desired_label not in model_classes:
        print(f"Desired label '{desired_label}' not found in the model's classes.")
        return None

    # Find the index of the desired label in the model's classes
    desired_label_index = list(loaded_model.classes_).index(desired_label)

    # Initialize variables to track the best word and highest confidence score
    best_word = None
    highest_confidence = -1.0  # Initialize with a very low value

    # Iterate through the words and find the one with the highest confidence
    for i, (word, word_lower) in enumerate(zip(word_list, word_list_lower)):
        confidence_score = proba_estimates[i][desired_label_index]
#         print(f"the word in find values word is {word} and confidence is {confidence_score}")
        if confidence_score > highest_confidence:
            best_word = word  # Return the original word with original case
            highest_confidence = confidence_score
#             print(f"the word in find values word is {word} and highest_confidence is {highest_confidence}")

    # Return the word with the highest confidence for the given label
    print(f"output got from value (find_values) svm is {best_word} for {desired_label}")
    return best_word




def update_tarining_data(PRIORITY_DATA,PRIORITY_CONTEXT,case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    temaplate_db=DB('template_db',**db_config)
    extraction_db=DB('extraction',**db_config)
    query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
    CUSTOMER_NAME = extraction_db.execute_(query)['customer_name'].to_list()[0]
    print(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
    if CUSTOMER_NAME:
        CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
        try:
            query = f"SELECT `PRIORITY_DATA` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
            data = temaplate_db.execute_(query)['PRIORITY_DATA'].to_list()[0]
            if data.empty:
                insert_data = {
                    'CUSTOMER_NAME': CUSTOMER_NAME,
                    'PRIORITY_DATA':json.dumps(PRIORITY_DATA),
                    'PRIORITY_CONTEXT':json.dumps(PRIORITY_CONTEXT)
                }
                temaplate_db.insert_dict(insert_data, 'trained_info')
        except:
            insert_data = {
                    'CUSTOMER_NAME': CUSTOMER_NAME,
                    'PRIORITY_DATA':json.dumps(PRIORITY_DATA),
                    'PRIORITY_CONTEXT':json.dumps(PRIORITY_CONTEXT)
                }
            temaplate_db.insert_dict(insert_data, 'trained_info')
    
    return True



def has_numerical(word):
    return any(char.isdigit() for char in word)


#---------------------------------------------------------------------------------------


def date_match(date):

    date_patterns = [
        r'\b\d{4}-\d{2}-\d{2}\b',         # YYYY-MM-DD
        r'\b\d{1,2}/\d{1,2}/\d{4}\b',     # MM/DD/YYYY
        r'\b\d{1,2}/\d{4}\b',              # DD/MM/YYYY
        r'\b\d{1,2}-\d{1,2}-\d{4}\b',     # DD-MM-YYYY
        r'\b\d{4}/\d{1,2}/\d{1,2}\b',      # YYYY/MM/DD
        r'\b\d{1,2}-\d{1,2}-\d{4}\b',     # MM-DD-YYYY
        r'\b\d{4}\.\d{2}\.\d{2}\b',        # YYYY.MM.DD
        r'\b\w+ \d{1,2}, \d{4}\b',        # Month DD, YYYY
        r'\b\d{1,2} \w+ \d{4}\b',         # DD Month YYYY
        r'\b\w+ \d{1,2} \d{4}\b',         # Month D, YYYY
        r'\b\d{1,2} \w+ \d{4}\b',         # D Month YYYY
        r'\b\w+ \d{1,2}th, \d{4}\b',      # Month DDth, YYYY
        r'\b\d{1,2}th \w+ \d{4}\b',       # DDth Month YYYY
        r'\b\w+ \d{1,2}th \d{4}\b',       # Month DDth YYYY
        r'\b\w+ \d{1,2} \d{4}\b',          # Month D YYYY
        r'\b\d{4} \w+\b',                  # YYYY Month
    ]

    # Combine patterns into a single regex
    combined_pattern = '|'.join(date_patterns)

    # Find and print dates in each sentence
    matches = re.findall(combined_pattern, date)
    if matches:
        return True
    else:
        return False


def date_extract(document_id_df_all,date_trained):
    
    if date_trained['bottom']>800 and date_trained['top']<0:
        return [],''
    
    date=date_trained['word']
    for words in document_id_df_all:
        # Sort the words by their 'top' position (vertical position)
        sorted_words = sorted(words, key=lambda x: x["top"])

        # Group words on the same horizontal line
        line_groups = []
        current_line = []
        for word in sorted_words:
            if not current_line:
                current_line.append(word)
            else:
                mid_word=abs(word["top"]+(word["height"]/2))
                mid_cu=abs(current_line[-1]["top"]+(current_line[-1]["height"]/2))
                diff=abs(mid_word - mid_cu)
                if diff < 2:
                    # Word is on the same line as the previous word
                    current_line.append(word)
                else:
                    # Word is on a new line
                    line_groups.append(current_line)
                    current_line = [word]

            # Add the last line to the groups
        if current_line:
            line_groups.append(current_line)

        # # Print the words grouped by horizontal lines
        for line in line_groups:
            if (line[0]['bottom'])>date_trained['bottom']+10 or (line[0]['top'])<date_trained['top']-10:
                continue
            line_words = [word["word"] for word in line]
            line_words=" ".join(line_words)

            date_valid=date_match(line_words)

            if date_valid:
                return line,line_words

    date_trained['bottom']=date_trained['bottom']+100
    date_trained['top']=date_trained['top']-100
    return date_extract(document_id_df_all,date_trained)
                


def non_standard_docs(case_id,tenant_id,seg):

    """
    Predicts non-standard values for each key in a document.

    Args:
        case_id: Case ID.
        tenant_id: Tenant ID.
        seg: Segment.

    Returns:
        dict: Predicted values.
    """

    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
    document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
    document_id_df_all=json.loads(document_id_df_all)
    predicted={}
    final_out={}
    predict_page={}
    high_dict={}
    page_no_dict={}
    head={}
    dates={}
    Sec_head={}

    prio_context,prio_data,prio_formula,user_trained_fields,date=get_training_data(case_id,tenant_id)
    print(F"date got is {date}")
    if date:
        extraction=DB('extraction',**db_config)
        max_cond=0
        date_extrac=''
        line_f=[]
        for words in document_id_df_all:
            # Sort the words by their 'top' position (vertical position)
            sorted_words = sorted(words, key=lambda x: x["top"])

            # Group words on the same horizontal line
            line_groups = []
            current_line = []
            for word in sorted_words:
                if not current_line:
                    current_line.append(word)
                else:
                    mid_word=abs(word["top"]+(word["height"]/2))
                    mid_cu=abs(current_line[-1]["top"]+(current_line[-1]["height"]/2))
                    diff=abs(mid_word - mid_cu)
                    if diff < 2:
                        # Word is on the same line as the previous word
                        current_line.append(word)
                    else:
                        # Word is on a new line
                        line_groups.append(current_line)
                        current_line = [word]

                # Add the last line to the groups
            if current_line:
                line_groups.append(current_line)
                
            # # Print the words grouped by horizontal lines
            for line in line_groups:
                line_words = [word["word"] for word in line]
                line_words=" ".join(line_words)
                try:
                    matcher = SequenceMatcher(None, line_words, date['word'])
                except:
                    matcher = SequenceMatcher(None, line_words, date)
                similarity_ratio_col = matcher.ratio()
                print(line_words,line_f,similarity_ratio_col,max_cond)
                if max_cond<similarity_ratio_col:
                    max_cond=similarity_ratio_col
                    date_extrac = line_words
                    line_f=line
        print(max_cond,line_f,date_extrac)
        date_valid=True
        if max_cond<0.5:
            date_valid=date_match(date_extrac)
        
        if not date_valid:
            line_f,date_extrac=date_extract(document_id_df_all,date)

        try:
            out_words=combine_dicts(line_f)
            print(F" ################## out_words is {out_words}")
            out_words['word']=date_extrac
            date_high=from_high_cor(out_words)
            print(F" ################## date_high is {date_high}")
            update_highlights(tenant_id,case_id,{"date_stat":date_high})
        except:
            pass

        print(F"date_extract got is {date_extrac}")

        query=f"update ocr set DATE_STAT = '{date_extrac}' where case_id ='{case_id}'"
        extraction.execute_(query)  


    #find all headers from all the pages once from loggesstic reggression
    for i,document_id_df in enumerate(document_id_df_all):
        try:
            print(F" ################ ")
            print(F" ################ ")
            print(f" ################ page is {i}")
            print(F" ################ ")
            print(F" ################ ")
            word_list=[]
            word_list_all=[]
            word_l_head=[]
            for word in document_id_df:
                if int(word['width'])<=1000:
                    word_list.append(word['word'])
                    if not has_numerical(word['word']):
                        word_l_head.append(word['word'])
                try:
                    wo=re.sub(r'[^a-zA-Z0-9]', '', word['word'])
                    wo=int(wo)
                except:
                    word_list_all.append(word['word'])

            out__=predict_with_svm(word_list_all,'non_stand_date')
            print(f"  ################ non_stand_date predicted is {out__}")
            for key1,value1 in out__.items():
                if key1 not in final_out:
                    final_out[key1]=value1
                    page_no_dict[key1]=str(i)
            dates[str(i)]=list(out__.keys())
                    
            out=predict_with_svm(word_list,'non_stand_col')
            print(f"  ################ non_stand_col predicted is {out}")
            final_out.update(out)

            for wo in out.keys():
               page_no_dict[wo]=str(i) 
            out_=predict_with_svm(word_l_head,'non_stand_head')
            print(f"  ################ non_stand_head predicted is {out_}")
            head[str(i)]=list(out_.keys())

            out_S=predict_with_svm(word_list,'non_stand_S_head')
            Sec_head[str(i)]=list(out_S.keys())
            print(f"  ################ non_stand_S_head predicted is {out_S}")

        except Exception as e:
            print(F"Exception is {e}")
            continue

    print(F" ################ ")
    print(F" ################ ")
    print(f" ################ final_out got is  {final_out}")
    print(f" ################ head is {head}")
    print(f" ################ Sec_head is {Sec_head}")
    print(f" ################ dates are {dates}")
    print(f" ################ page_no_dict is {page_no_dict}")
    
    
    json_file_path = f"/var/www/extraction_api/app/extraction_folder/non_stand/extract_variables.json"
    corpus = read_corpus_from_json(json_file_path)
    #env variables that can be changed based on the requriements:
    level_2_keys_lables=corpus['level_2_keys_lables']
    date_range_extract=corpus['date_range_extract']
    general_key_varaibles=corpus['general_key_varaibles']
    level_2_keys_list=corpus['level_2_keys_list']

    for key,lable in final_out.items():
        if lable in level_2_keys_lables:
            level_2_keys_list.append(key)

    print(f" ################ level_2_keys_list is {level_2_keys_list}")
    print(F" ################ ")
    print(F" ################ ")

    #here we will send all the best keys that we finalised to this function for further extraction process
    print(f" ################ entering into find_non_standard_values")
    predicted,predict_page,high_dict,column_head_dict=find_non_standard_values(final_out,document_id_df_all,seg,head,predicted,page_no_dict,predict_page,level_2_keys_lables,dates,high_dict,date_range_extract,general_key_varaibles,Sec_head)
    print(f" ################ predicted values are {predicted}")

    extarction_db = DB('extraction', **db_config)    
    insert_data = {
            'column_head_dict':json.dumps(column_head_dict),
            'predicted':json.dumps(predicted),
            'predict_page':json.dumps(predict_page),
            'high_dict':json.dumps(high_dict),
            'seg':''
        }
    extarction_db.update('ocr', update=insert_data, where={'case_id': case_id})

    try:
        update_db_ntable({},case_id,tenant_id,'Broker')
    except Exception as e:
        print(F"Exception is {e}")

    

    return predicted


# Function to calculate the horizontal distance between two words based on their left and right coordinates
def horizontal_distance(word1, word2):
    if word2['left'] > word1['right']:
        return word1['right'] - word2['left']
    else:
        return word2['right'] - word1['left']
# Function to calculate the vertical overlap between two words (top and bottom)
def vertical_distance(word1, word2):
    return word2['top'] - word1['bottom']

# Function to group words based on both horizontal proximity and vertical overlap
def group_words_by_proximity(words, y_threshold=15):
    # Sort words by their left coordinate (left to right, top to bottom)
    words = sorted(words, key=lambda w: (w['top'], w['left']))

    grouped_words = []
    picked=[]
    for i in range(len(words)-1):
        if i+1 == len(words):
            break
        current_group=[]
        if words[i] not in picked:
            current_group.append(words[i])
            picked.append(words[i])
        for j in range(i+1,len(words)-1):
            if (words[i]["left"] <= words[j]["left"]<= words[i]["right"] or words[i]["left"] <= words[j]["right"] <= words[i]["right"]) and vertical_distance(words[i], words[j]) < y_threshold:
                if words[j] not in picked:
                    current_group.append(words[j])
                    picked.append(words[j])

        grouped_words.append(current_group)

    return grouped_words


def find_table_user_trained_header(ocr_data,headers,cont,head_dis):

    print(f" ################ header got is {headers}")
  
    head_list=[]
    for word in ocr_data:
        for head in headers:
            print(f" ############## head is {head} and word is {word['word']}")
            matcher = SequenceMatcher(None, head, word['word'])
            similarity_ratio_head = matcher.ratio()
            if similarity_ratio_head>0.9:
                    head_list.append(word)
    if len(head_list)>1:
        grouped_headers=group_words_by_proximity(head_list)
    else:
        grouped_headers=[head_list]

    print(f" ############ here Grouped headers are {grouped_headers}")

    final_head_group=[] 
    max_conf_diff=1000
    head_join=' '.join(headers)
    for group in grouped_headers:
        if not group:
            continue
        group_words=[]
        for gr_word in group_words:
            group_words.append(gr_word["word"])
        group_join=' '.join(group_words)

        matcher = SequenceMatcher(None, group_join,head_join)
        similarity_ratio_head = matcher.ratio()
        if similarity_ratio_head>0.6:
            head_list.append(word)

        print(f" ############ here group_join has minimum thr are  {group_join}")

        group_box=combine_dicts(group)
        present_diff=calculate_distance(cont,group_box)
        confidence_diff=abs(present_diff-head_dis)
        if max_conf_diff>confidence_diff:
            final_head_group=group
            max_conf_diff=confidence_diff

    header_box={}    
    if final_head_group:
        header_box=combine_dicts(final_head_group)
    
    return header_box



def calculate_value_distance(point1, point2):
    # print(point1,point2)
    x1 = (point1['right']-point1['left'])/2
    x2= (point2['right']- point2['left'])/2
    distance = abs(x1-x2)
    return distance


def extract_user_trained_fields(ocr_data_all,user_trained_fields):

    predicted_fields={}
    high={}

    try:
        user_trained_fields=user_trained_fields[0]
        user_trained_fields=json.loads(user_trained_fields)
    except:
        user_trained_fields=user_trained_fields


    for predict,trained_dict in user_trained_fields.items():

        predicted_fields[predict]={}
        
        try:
            diff_col=trained_dict[0]
            head_dis=trained_dict[1]
            cont=trained_dict[4]
            distance=cont.get('hypth',0)
            col=trained_dict[2]
            head=trained_dict[3]
            diff_value_head=trained_dict[5]
            try:
                enhanced_value=trained_dict[6]
            except:
                enhanced_value=False
        except:
            continue

        if not cont or not head or not col:
            print(f" #################### insuffieent user trained data for {predict}")
            continue
        
        column_dict,context=get_col(col,cont,ocr_data_all,distance,False)

        print(f" ########### here we got column and context {column_dict}  {context}")

        ocr_data=ocr_data_all[column_dict['pg_no']-1]
        excceded_min_flag=False
        if diff_col:
            next_values,excceded_min_flag,min_diff=find_next_ele_crop(column_dict,ocr_data,float(diff_col))
        else:
            next_values,excceded_min_flag,min_diff=find_next_ele_crop(column_dict,ocr_data,float(0))
            if min_diff>10:
                next_values=[]
                        
        print(f" ################ next_values got is {next_values}")

        #we we will filter all the values that are found right next to the key
        numerical_values=[]
        if next_values:
            # numerical_values=filter_num(next_values,True)
            numerical_values = sorted(next_values, key=lambda x: x["right"])
        
        print(f"################ numerical_values after filtering {numerical_values}")
        try:
            header_box=find_table_user_trained_header(ocr_data,head,context,head_dis)
        except:
            header_box=None

        print(f"################ header_box after  {header_box}")

        if header_box:
            max_calculated_diff=10000
            predicted={}
            for value in numerical_values:
                filtered_value=re.sub(r'[^0-9,.]', '', column_dict)
                if not filtered_value:
                    continue
                if not enhanced_value:
                    diff_value_header=calculate_distance(header_box,value)
                else:
                    diff_value_header=calculate_value_distance(header_box,value)
                calculated_diff=abs(diff_value_head-diff_value_header)
                print(f"################ calculated_diff is  {calculated_diff} for {value}")
                if max_calculated_diff>calculated_diff:
                    max_calculated_diff=calculated_diff
                    predicted=value
            print(f"################ final predict is  {predicted}")
            if not predicted and (excceded_min_flag or not numerical_values) :
                high[predict]=from_high_cor(column_dict)
                predicted_fields[predict]=re.sub(r'[^0-9,.-]', '', column_dict['word'])
            else:
                if predicted:
                    predicted_fields[predict]=predicted['word']
                    high[predict]=from_high_cor(predicted)

    return predicted_fields,high
    

@app.route('/prio_funtion', methods=['POST', 'GET'])
def prio_funtion():

    data = request.json
    print(f"Data recieved: {data}")
    try:
        tenant_id = data['tenant_id']
        try:
            case_id= data['email']['case_id']
        except:
            case_id = data.get('case_id', None)

        seg = data.get('segment', None)
        user = data.get('user', None)
        session_id = data.get('session_id', None)
    except Exception as e:
        logging.warning(f'## TE Received unknown data. [{data}] [{e}]')
        return {'flag': False, 'message': 'Incorrect Data in request'}
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    extraction_db = DB('extraction', **db_config)

    query = f"SELECT `seg` from  `ocr` where `case_id` = '{case_id}'"
    seg = extraction_db.execute_(query)['seg'].to_list()[0]
    print(f"seg got is {seg}")

    if not seg:

        query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
        document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
        document_id_df_all=json.loads(document_id_df_all)

        query = f"SELECT `predict_page`,`predicted`,`high_dict`,`column_head_dict` from  `ocr` where `case_id` = '{case_id}'"
        data = extraction_db.execute_(query)
        
        predict_page=json.loads(data['predict_page'].to_list()[0])
        predicted=json.loads(data['predicted'].to_list()[0])
        high_dict=json.loads(data['high_dict'].to_list()[0])
        column_head_dict=json.loads(data['column_head_dict'].to_list()[0])

        print(F" ################ predict_page is {predict_page}")
        print(F" ################ high_dict is {high_dict}")
        print(F" ################ predicted is {predicted}")
        print(F" ################ column_head_dict is {column_head_dict}")

        #priority function
        print(F" ################ starting the priority_func")
        predicted_priority,training,croped_high,flag,user_trained_fields=priority_func(seg,high_dict,case_id,tenant_id,document_id_df_all,predict_page)
        print(F" ################ after priority function output for predicted_priority is {predicted_priority} and training is {training}")
        
        user_trained_high={}
        user_trained_predicted_fields={}
        if user_trained_fields:
            user_trained_predicted_fields,user_trained_high=extract_user_trained_fields(document_id_df_all,user_trained_fields)

        if flag:
            
            print(F" ################ getting context")
            predicted_context=get_context(high_dict,predict_page,column_head_dict,document_id_df_all,seg)
            print(F" ############### final predicted_context for all predicted fields is {predicted_context}")

            #function that store data in tempalte db
            print(F" ################ flag is True so upadating the training data")
            update_tarining_data(training,predicted_context,case_id,tenant_id)

        predicted_priority.update(user_trained_predicted_fields)

        #we will update the predicted values in to the database
        try:
            print(F" ################ upadating into database")
            update_db_ntable(predicted_priority,case_id,tenant_id)
        except Exception as e:
            print(F" ################ Exception is {e}")

        #here this fucntion generate the highlights for the predcited values
        print(F" ################ getting highlights")
        high=get_highlight_n_table(high_dict,predict_page,document_id_df_all,seg)
        print(F" ################ croped_high are {croped_high}")
        high.update(croped_high)
        high.update(user_trained_high)
        print(F"  ################ final high_dict for all predicted fields is {high}")
        #we will update the generated highlights to the database
        update_highlights(tenant_id,case_id,high)

    else:
        print(F"  ################ Standard format and seg is {seg}")

    return {"data":{"message":"prio api is sucessfull."},"flag":True}


#---------------------------------------------------------------------------------------------


def get_divivsion_labels(word_list,seg):
    model_filename = f"/var/www/extraction_api/app/extraction_folder/{seg}/{seg}_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/{seg}/{seg}_count_vectorizer.joblib"

    try:
        # Load the model
        clf = joblib.load(model_filename)

        # Load the vectorizer
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(f"######################### joblib file for this seg is missing {model_filename} and {vectorizer_filename}")
        return {}

    # Convert the input list of words to a bag of words (BoW) representation
    X_inference = vectorizer.transform(word_list)

    # Make predictions using the loaded model
    predictions = clf.predict(X_inference)
    
    output={}
    # print the predictions for each word
    
    for word, prediction in zip(word_list, predictions):
#         print(f"Word: {word}, Predicted Label: {prediction}")
        if prediction !='other':
            output[word]=prediction
    return output


#--------------------------------------------------------------------------------------------


def enhance_divisions_hor(ocr_data, headers, head_l):
    """
    Enhances horizontal divisions based on headers and distances.

    Args:
        ocr_data (list): List of dictionaries containing OCR data.
        headers (dict): Dictionary containing headers.
        head_l (list): List containing two headers.

    Returns:
        dict: Enhanced divisions.
    """
    out = {}
    head_1 = head_l[0]
    head_2 = head_l[1]
    
    # Determine the dividing line between headers
    if head_1['left'] > head_2['left']:
        div_line = abs(head_1['right'] + head_2['left']) / 2
    else:
        div_line = abs(head_2['right'] + head_1['left']) / 2
    
    div_1 = []
    div_2 = []
    
    # Split OCR data into two divisions based on the dividing line
    for word in ocr_data:
        if word['left'] < div_line and word['right'] < div_line:
            div_1.append(word)
        elif word['left'] > div_line and word['right'] > div_line:
            div_2.append(word)
    
    # Iterate through headers and check if they are present in each division
    for key, value in headers.items():
        # Check if header is present in the first division
        phrase_present = any(entry['word'] == value['word'] for entry in div_1)
        if phrase_present:
            out[key] = [div_1]
        # Check if header is present in the second division
        phrase_present = any(entry['word'] == value['word'] for entry in div_2)
        if phrase_present:
            out[key] = [div_2]
    
    return out


#---------------------------------------------------------------------------------------------


def has_numeric_word(data_list):
    """
    Checks if any word in the list of dictionaries contains a numeric character.

    Args:
        data_list (list): List of dictionaries containing OCR data.

    Returns:
        bool: True if a numeric character is found, False otherwise.
    """
    # Iterate through each dictionary in the list
    for data_dict in data_list:
        # Check if the dictionary contains the key 'word' and if any character in the word is numeric
        if 'word' in data_dict and any(char.isdigit() for char in data_dict['word']):
            print(F'retuning true for the {data_dict}')
            return True  # Return True if a numeric character is found
    return False  # Return False if no numeric character is found in any word


#---------------------------------------------------------------------------------------------


def next_line_check(next_line, div, seg):
    """
    Checks if the next line contains relevant information based on predicted headers and numeric words.

    Args:
        next_line (list): List of dictionaries containing OCR data for the next line.
        div (str): Division identifier.
        seg (str): Segment identifier.

    Returns:
        bool: True if the next line contains relevant information, False otherwise.
    """
    # Initialize an empty list to store words from the next line
    word_list = []
    
    # Extract words from each dictionary in the next line and append them to the word_list
    for word in next_line:
        word_list.append(word['word'])
        
    # Predict headers and fields using the word list, division, and segment
    out = predict_table_headers(word_list, div, seg)
    
    # Extract field and header keywords from the prediction output
    if out:
        field = out['field_keywords']
        header = out['header_keywords']
    else:
        field = []
        header = []
    
    # If field keywords are found in the prediction output, return True
    if field:
        return True
    else:
        # If only one header keyword is found and the next line contains numeric words, return True
        if len(header) == 1:
            if has_numeric_word(next_line):
                return True
            else:
                return False
        else:
            return False

        
#-----------------------------------------------------------------------------------------
    
    
def enhance_divisions_ver(words_, div, seg):
    """
    Enhances vertical divisions in OCR data by combining lines that are close to each other.

    Args:
        words_ (list): List of dictionaries containing OCR data for words.
        div (str): Division identifier.
        seg (str): Segment identifier.

    Returns:
        list: Enhanced divisions after combining close lines.
    """
    out = []
    # Sort words by their "top" position
    sorted_words = sorted(words_, key=lambda x: x["top"])
    # Initialize variables
    line_groups = []
    current_line = []

    # Group words on the same vertical line
    for word in sorted_words:
        if not current_line:
            current_line.append(word)
        else:
            diff = abs(word["top"] - current_line[-1]["top"])
            if diff < 7:
                current_line.append(word)
            else:
                line_groups.append(current_line)
                current_line = [word]
    
    # Append the last line to the line groups
    if current_line:
        line_groups.append(current_line)
    
    # Initialize variables for combining lines
    first = line_groups[0]
    average_bottom = first[0]["bottom"]
    height_values = [item['height'] for item in sorted_words]
    
    # Calculate the minimum height based on the most common height value
    try:
        if seg == 'WBG':
            min_height = 10
        else:
            min_height = statistics.mode(height_values)
    except:
        min_height = 7
    
    temp = []
    temp.extend(first)

    # Iterate through line groups to combine close lines
    for i in range(1, len(line_groups)):
        line = sorted(line_groups[i], key=lambda x: x["left"])
        average_top = line[0]["top"]
        
        # Iterate through the list to find the greatest "top" value
        for item in line:
            if item["top"] < average_top:
                average_top = item["top"]
        
        gap = abs(average_bottom - average_top)
        
        # Check if the gap between lines is less than the minimum height
        if (gap - min_height) < 5:
            if temp:
                temp.extend(line)
            else:
                temp = line
        else:
            # Check if the next line contains relevant information
            if next_line_check(line, div, seg):
                if temp:
                    temp.extend(line)
                else:
                    temp = line
            else:
                line_words = [word["word"] for word in temp]
                combined_string = ' '.join(line_words)
                out.append(temp)
                temp = line
        
        average_bottom = line[0]["bottom"]
        
        # Iterate through the list to find the minimum "bottom" value
        for item in line:
            if item["bottom"] > average_bottom:
                average_bottom = item["bottom"]
    
    # Append the last combined lines to the output
    if temp:
        out.append(temp)
        
    return out



#-----------------------------------------------------------------------------------------


def get_sub_labels(word_list, seg):
    """
    Predict sub-labels for a list of words using a logistic regression model.

    Args:
        word_list (list): List of words to predict sub-labels for.
        seg (str): Segment identifier.

    Returns:
        dict: Dictionary containing predicted sub-labels for the input words.
    """
    # Define the paths to the model and vectorizer files
    model_filename = f'/var/www/extraction_api/app/extraction_folder/{seg}/{seg}_lh_logistic_regression_model.joblib'
    vectorizer_filename = f'/var/www/extraction_api/app/extraction_folder/{seg}/{seg}_lh_count_vectorizer.joblib'

    try:
        # Load the logistic regression model
        clf = joblib.load(model_filename)

        # Load the count vectorizer
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(f"Joblib files for segment {seg} are missing: {model_filename} and {vectorizer_filename}")
        return {}

    # Convert the input list of words to a bag of words (BoW) representation
    X_inference = vectorizer.transform(word_list)

    # Make predictions using the loaded model
    predictions = clf.predict(X_inference)
    
    output = {}
    
    # Populate the output dictionary with predicted sub-labels for non-'other' predictions
    for word, prediction in zip(word_list, predictions):
        if prediction != 'other':
            output[word] = prediction
    
    return output



#---------------------------------------------------------------------------------------------


def predict_sub_divisions(predcit,ocr_word,seg):
    """
    Predict sub-divisions based on the provided prediction and OCR words.

    Args:
        predict (dict): Predictions for OCR words.
        ocr_word (list): List of OCR words.
        seg (str): Segment identifier.

    Returns:
        list: List of sub-divisions based on the predictions and OCR words.
    """
    out_dict=[]
    word_list=[]
    for word in ocr_word:
        word_list.append(word['word'])
    division=get_sub_labels(word_list,seg)
    print(f"divisions sub div is {division} \n")
    if not division:
        return {}
    list_=[]
    for word in ocr_word:
        if word['word'] in division:
            list_.append(word)
    list_ = sorted(list_, key=lambda x: x['top'])
    # print(list_)
    for i in range(0,len(list_)):
        # print(f'i is {i} and list is {list_[i]}')
        if i==len(list_)-1:
            bottom_word={}
            table_bottom=1000
        else:
            bottom_word=list_[i+1]
            table_bottom=bottom_word['top']
        top_word=list_[i]
        table_top=top_word['top']
        temp=[]
        # print(F'top and bottom are {table_top} ,{table_bottom}')
        if abs(table_top-table_bottom)<10:
            i=i+1
            if i==len(list_)-1:
                table_bottom=10000
            else:
                table_bottom=list_[i+1]['top']
        for words in ocr_word:
            if words['top']>=table_top-5 and words['top'] < table_bottom:
                temp.append(words)
        if list_[i] not in temp:
            temp.append(list_[i])
        # print(temp)
        out_dict.append(temp)
    return out_dict


#-----------------------------------------------------------------------------------------


def predict_table_divisions(ocr_words, words, seg):
    """
    Predict table divisions based on the OCR words and segment identifier.

    Args:
        ocr_words (list): List of OCR words.
        words (list): List of words.
        seg (str): Segment identifier.

    Returns:
        dict: Predicted table divisions.
    """
    out_dict = {}
    
    # Get division labels using the get_divivsion_labels function
    division = get_divivsion_labels(words, seg)
    print(f"Divisions obtained from logistic regression: {division}\n")
    
    list_ = [word for word in ocr_words if word['word'] in division]
    list_ = sorted(list_, key=lambda x: x['top'])
    
    flag = 0
    for i in range(len(list_)):
        if flag == 1:
            print(F"Skipping this iteration")
            flag = 0
            continue
        
        if i == len(list_) - 1:
            bottom_word = {}
            table_bottom = 1000
        else:
            bottom_word = list_[i + 1]
            table_bottom = bottom_word['top']
        
        top_word = list_[i]
        table_top = top_word['top']
        temp = []
        temp__ = []
        
        if abs(table_top - table_bottom) < 10:
            i = i + 1
            if i == len(list_) - 1:
                table_bottom = 10000
                flag = 1
            else:
                table_bottom = list_[i + 1]['top']
            
            # Collect OCR words within the division
            for words in ocr_words:
                if table_top + 5 <= words['top'] < table_bottom:
                    temp__.append(words)
            
            temp__.append(top_word)
            temp__.append(bottom_word)
            
            headers = {division[top_word['word']]: top_word, division[bottom_word['word']]: bottom_word}
            head_l = [top_word, bottom_word]
            
            # Enhance horizontal divisions
            divs = enhance_divisions_hor(temp__, headers, head_l)
            out_dict.update(divs) 
        else:
            # Collect OCR words within the division
            for words in ocr_words:
                if table_top - 5 <= words['top'] < table_bottom:
                    temp.append(words)
            
            division_label = division[top_word['word']]
            
            if division_label not in out_dict:
                out_dict[division_label] = [temp]
            else:
                out_dict[division_label].append(temp)
        
    return out_dict


#---------------------------------------------------------------------------------------------


def predict_table_headers(word_list, div, seg):
    """
    Predict table headers based on the given list of words, division, and segment.

    Args:
        word_list (list): List of words.
        div (str): Division identifier.
        seg (str): Segment identifier.

    Returns:
        dict: Predicted table headers.
    """
    # Load the trained model and vectorizer
    model_filename = f"/var/www/extraction_api/app/extraction_folder/{seg}/{div}_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/{seg}/{div}_count_vectorizer.joblib"
    
    try:
        # Load the model
        clf = joblib.load(model_filename)

        # Load the vectorizer
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(f"Joblib file for this division is missing: {vectorizer_filename}, {model_filename}")
        return {}

    # Convert the input list of words to a bag of words (BoW) representation
    X_inference = vectorizer.transform(word_list)

    # Make predictions using the loaded model
    predictions = clf.predict(X_inference)
    
    field_keywords = {'field_keywords': [], 'header_keywords': [], 'table_headers': []}
    
    # Iterate over the predictions for each word
    for word, prediction in zip(word_list, predictions):
        if prediction == 'CH':
            field_keywords['field_keywords'].append(word)
        elif prediction == 'RH':
            field_keywords['header_keywords'].append(word)
        elif prediction == 'LH':
            field_keywords['table_headers'].append(word)
    
    return field_keywords


#--------------------------------------------------------------------------------------------------


def find_next_ele_below(each_value, ocr_data):
    """
    Find the elements present below a given element in the OCR data.

    Args:
        each_value (dict): Dictionary representing the given element.
        ocr_data (list): List of dictionaries representing OCR data.

    Returns:
        list: List of elements present below the given element.
    """
    return_list = []
    centroid = (each_value['left'] + each_value['right']) / 2
    
    # Iterate over each word in the OCR data
    for word in ocr_data:
        # Check if the word is below the centroid of the given element
        if (word['left'] <= centroid <= word['right'] or
            word['left'] <= each_value['right'] <= word['right'] or
            word['left'] <= each_value['left'] <= word['right']) and word['top'] > each_value['top']:
            return_list.append(word)
        elif (word['left'] <= each_value['left'] - 10 <= word['right']) and word['top'] > each_value['top']:
            return_list.append(word)
        elif (word['left'] <= each_value['right'] + 10 <= word['right']) and word['top'] > each_value['top']:
            return_list.append(word)
        elif ((each_value['right'] < word['left'] < each_value['right']) or
              (each_value['right'] < word['right'] < each_value['right']) or
              (each_value['right'] < (word['left'] + word['right']) / 2 < each_value['right'])) and word['top'] > each_value['top']:
            return_list.append(word)
    
    ret = []
    first = each_value
    
    # Iterate over the returned list to filter elements based on the top difference
    if return_list:
        for ele in return_list:
            diff_top = abs(first['bottom'] - ele['top'])
            if diff_top < 100:
                ret.append(ele)
                first = ele
        return ret
    else:
        return []


#--------------------------------------------------------------------------------------------------


def find_next_ele_stan(key, ocr_data):
    """
    Find the elements that appear next to a given key element in the OCR data.

    Args:
        key (dict): Dictionary representing the given key element.
        ocr_data (list): List of dictionaries representing OCR data.

    Returns:
        list: List of elements that appear next to the given key element.
    """
    next_values = []
    # Sort the OCR data by the 'top' coordinate
    sorted_words = sorted(ocr_data, key=lambda x: x["top"])
    k_right = key["right"]  # Right coordinate of the key element
    k_top=key["top"]

    # Iterate over each word in the sorted OCR data
    for word in sorted_words:
        # Calculate the vertical difference between the current word and the key element
        diff_ver = abs(word["top"] - key["top"])
        
        # Check if the word is on the same line as the key element and appears to the right of it
        if diff_ver < 7 and k_right < word["left"]:
            next_values.append(word)  # Add the word to the list of next values
            
    return next_values


def find_before_ele(key, ocr_data):
    """
    Find elements that appear before a specified key element in the OCR data.

    Args:
        key (dict): Dictionary representing the specified key element.
        ocr_data (list): List of dictionaries representing OCR data.

    Returns:
        list: List of words that appear before the specified key element.
    """
    before_values = []
    # Sort the OCR data by the 'top' coordinate
    sorted_words = sorted(ocr_data, key=lambda x: x["top"])
    k_left = key["left"]  # Left coordinate of the key element

    # Iterate over each word in the sorted OCR data
    for word in sorted_words:
        # Calculate the vertical difference between the current word and the key element
        diff_ver = abs(word["top"] - key["top"])
        
        # Check if the word is on the same line as the key element and appears to the left of it
        if diff_ver < 5 and k_left > word["left"]:
            before_values.append(word['word'])  # Add the word to the list of before values
            
    return before_values

 
#--------------------------------------------------------------------------------------------------


def find_table_header(each_value, ocr_data, header, predict='', seg='', api=True):
    """
    Find the header of a table element based on its position and nearby words in the OCR data.

    Args:
        each_value (dict): Dictionary representing the table element.
        ocr_data (list): List of dictionaries representing OCR data.
        header (list): List of header words to search for.
        found_al (list): List of words already found.
        predict (str): Optional parameter for prediction.
        seg (str): Optional parameter for segmentation.
        api (bool): Optional parameter to enable/disable API usage.

    Returns:
        str: The identified header word.
    """
    return_list = []  # List to store candidate header words
    centroid = (each_value['left'] + each_value['right']) / 2  # Calculate centroid of the table element
    head = []  # List to store header words found to the left of the table element
    
    # Iterate over each word in the OCR data
    for word in ocr_data:
        # Check if the word is in the header and not already found
        if word['word'] in header:
            # Check if the word is horizontally aligned with the centroid of the table element
            if (word['left'] <= centroid <= word['right'] or 
                word['left'] <= each_value['right'] <= word['right'] or 
                word['left'] <= each_value['left'] <= word['right']) and word['top'] < each_value['top']:
                return_list.append(word)  # Add the word to the candidate list
            # Check if the word is to the left of the table element and above it
            elif (word['left'] <= each_value['left'] <= word['right']) and word['top'] < each_value['top']:
                return_list.append(word)  # Add the word to the candidate list
            # Check if the word is to the right of the table element and above it
            elif (word['left'] <= each_value['right'] <= word['right']) and word['top'] < each_value['top']:
                return_list.append(word)  # Add the word to the candidate list
            # If API usage is enabled, consider words to the left of the table element as potential headers
            elif (word['right'] < each_value['left']) and word['top'] < each_value['top'] and api:
                head.append(word)  # Add the word to the header list

    # print the results for debugging purposes
    print(f"Result found is {return_list} and head is {head}")
    
    # If candidate headers are found, return the nearest one based on vertical proximity
    if return_list:
        nearest_dict = min(return_list, key=lambda x: abs(x['top'] - each_value['top']))
        return nearest_dict['word']
    else:
        # If no candidate headers are found, check for headers to the left of the table element
        if head:
            nearest_dict = min(head, key=lambda x: abs(x['right'] - each_value['left']))
            return nearest_dict['word']  # Return the nearest header found to the left
        else:
            return ''  # Return an empty string if no header is found


#--------------------------------------------------------------------------------------------------
    

def get_divided_data(data):

    """
    Split a word into smaller chunks based on a specific pattern.

    Args:
        data (dict): Dictionary representing the word to be divided.

    Returns:
        list: List of dictionaries representing the divided words.
    """

    word = data["word"]
    width=abs(data["right"]-data["left"])
    word_len=len(data['word'])
    split_words = []
    start = 0
    for i in range(len(word) - 3):
        if word[i:i+4] == "days":
            split_words.append(word[start:i+4])
            start = i+4
    split_words.append(word[start:])
    out=[]
    out_right=0
    for word in split_words:
        if word:
            temp={}
            temp['word']=str(word)
            if out_right!=0:
                temp['left']=out_right
            else:
                temp['left']=data['left']
            out_wid=(len(word)/word_len)*width
            temp['width']=out_wid
            temp['right']=out_wid+temp['left']
            out_right=temp['right']
            temp['height']=data['height']
            temp['top']=data['top']
            temp['bottom']=data['bottom']
            temp['confidence']=data['confidence']
            temp['sen_no']=data['sen_no']
            temp['pg_no']=data['pg_no']
            temp['x-space']=data['x-space']
            out.append(temp)
    return out


#---------------------------------------------------------------------------------------------


def check_headers(head,data):
    """
    Process a list of words, identifying headers and splitting them if they contain numeric characters.

    Args:
        head (list): List of header words.
        data (list): List of dictionaries representing words.

    Returns:
        tuple: A tuple containing a list of processed words and a list of new header words.
    """
    ret=[]
    new_head=[]
    for word in data:
        if word['word'] in head:
            value = re.sub(r'[^0-9]', ' ', word['word'])
            if value:
                numbers_list = value.split()
                number_of_numbers = len(numbers_list)
                if number_of_numbers>2:
                    out_w=get_divided_data(word)
                    ret.extend(out_w)
                    for val in out_w:
                        new_head.append(val['word'])
                elif number_of_numbers==2:
                    if '-' in word['word']:
                        new_head.append(word['word'])
                        ret.append(word)
                    else:
                        out_w=get_divided_data(word)
                        ret.extend(out_w)
                        for val in out_w:
                            new_head.append(val['word'])
                else:
                    new_head.append(word['word'])
                    ret.append(word)
            else:
                out_w=get_divided_data(word)
                ret.extend(out_w)
                for val in out_w:
                    new_head.append(val['word'])
        else:
            ret.append(word)
    return ret,new_head


#-------------------------------------------------------------------------------------------------


def custom_mode(data):

    """
    Calculate the mode (most frequently occurring value) of a list of data.

    Args:
        data (list): List of values.

    Returns:
        list: List containing the mode(s).
    """

    count = Counter(data)
    max_count = max(count.values())
    mode = [key for key, value in count.items() if value == max_count]
    return mode


#---------------------------------------------------------------------------------------------


def enhance_headers(headers,word_div):

    """
    Enhance headers by categorizing them into "field keywords" and "header keywords" based on their position relative to certain lines on the document.

    Args:
        headers (dict): Dictionary containing 'field_keywords', 'header_keywords', and 'table_headers'.
        word_div (list): List of word dictionaries containing information about the words.

    Returns:
        tuple: A tuple containing the enhanced headers dictionary and a sorted list of header keywords.
    """

    print(f"startring enhancing headers")
    out_list={'field_keywords':[],'header_keywords':[]}
    field=headers['field_keywords']
    header=headers['header_keywords']
    table_headers=headers['table_headers']
    field_list=[]
    head_list=[]
    sort_list={}
    for word in word_div:
        if word['word'] in field:
            field_list.append(word['left'])
        elif word['word'] in header:
            head_list.append(word['top'])
    print(f"field_list got is {headers}")
    field_list = [(int(number // 100) * 100) + ((int(number // 10) % 10) *10)  for number in field_list]
    if field_list:
        mode_=custom_mode(field_list)
        if len(mode_)>1:
            field_line=min(mode_)
        else:
            field_line=mode_[0]
    else:
        return headers
    head_list = [(int(number // 100) * 100) + ((int(number // 10) % 10) *10)  for number in head_list]
    if head_list:
        mode_=custom_mode(head_list)
        if len(mode_)>1:
            head_line=sum(mode_) / len(mode_)
        else:
            head_line=mode_[0]
    else:
        return headers
    out_words=[]
    right=[]
    for word in word_div:
        print(f"for the word {word['word']}")
        if word['word'] in field:
            print(F"int the if")
            dif_line=abs(word['left']-field_line)
            print(F"int the field_line {field_line}  word['left'] {word['left']} dif_line is{dif_line}")
            if dif_line< 100:
                out_list['field_keywords'].append(word['word'])
                right.append(word['right'])
            else:
                dif_top=abs(word['top']-head_line)
                
                print(F"int the dif_top {dif_top} word['top'] {word['top']} head_line {head_line}")
                if dif_top <= 15:
                    out_list['header_keywords'].append(word['word'])
                    sort_list[word['word']]=word['left']
                else:
                    out_words.append(word)
            print(f"out_list is {out_list}")
        elif word['word'] in header:
            print(f"in the else")
            
            dif_top=abs(word['top']-head_line)
            print(F"int the dif_top {dif_top} word['top'] {word['top']} head_line {head_line}")
            if dif_top<=15:
                out_list['header_keywords'].append(word['word'])
                sort_list[word['word']]=word['left']
            else:
                dif_line=abs(word['left']-field_line)
                print(F"int the field_line {field_line}  word['left'] {word['left']} dif_line is{dif_line}")
                if dif_line< 100:
                    out_list['field_keywords'].append(word['word'])
                    right.append(word['right'])
                else:
                    out_words.append(word)
            print(f"out_list is {out_list}")
    if out_words:
        right_most=max(right)
        mid=right_most-field_line
        for word in out_words:
            diff_mid=abs(word['left']-mid)
            if diff_mid<=100:
                out_list['field_keywords'].append(word['word'])
            elif head_line>word['top']:
                out_list['header_keywords'].append(word['word'])
                sort_list[word['word']]=word['left']


    out_list['table_headers']=table_headers
    return out_list,sort_list


#--------------------------------------------------------------------------------------------------


def remove_from_ocr(remove_words,ocr_data_parse):
    """
    Remove specific words from OCR data.

    Args:
        remove_words (str): Word to be removed.
        ocr_data_parse (list): List of dictionaries containing OCR data.

    Returns:
        list: List of dictionaries after removing the specified word.
    """
    ocr_data_deleted=[]
    for ocr_data in ocr_data_parse:
        if remove_words != ocr_data['word']:
            # print(f"#### matched word is {word}m###### text is {ocr_data['word']}")
            if ocr_data not in ocr_data_deleted:
                ocr_data_deleted.append(ocr_data)
    return ocr_data_deleted


#-----------------------------------------------------------------------------------------------------


def get_head_alias(table_headers,seg):
    """
    Extracts aliases for table headers.

    Args:
        table_headers (list): List of table headers.
        seg (str): Segment identifier.

    Returns:
        str: Alias for the table headers.
    """
    print(f"header send  is {table_headers}")
    value=''
    for key in table_headers:
        pattern = r'\b\d+\b'
        values = re.findall(pattern, key)
        if len(values) == 2:
            value1, value2 = map(int, values)
            value=str(value1)+'-'+str(value2)
        elif len(values) == 1:
            if 'more' in key or "greater" in key:
                value=(">"+str(values[0]))
            else:
                value=("<"+str(values[0]))
    print(f"returing value is {value}")
    return value


def custom_1(predicted_fields):
    """
    Transforms predicted fields into a tabular format.

    Args:
        predicted_fields (dict): Predicted fields.

    Returns:
        list: Transformed data in a tabular format.
    """
    transformed_data = []

    # Iterate through predicted fields
    for key, value in predicted_fields.items():
        # Initialize a row with the 'Particulars' key
        row = {'Particulars': key}
        # If the value is not empty, add it to the row
        if value:
            for header, content in value[0].items():
                row[header] = content
        transformed_data.append(row)

    # Extract headers from the first row
    headers = [k for k, v in transformed_data[0].items()]

    # Construct the final table
    final_table = [{"header": headers, "rowData": transformed_data}]

    return final_table


def custom_2(predicted_fields):
    """
    Transforms predicted fields into a tabular format with multiple columns for each field.

    Args:
        predicted_fields (dict): Predicted fields.

    Returns:
        list: Transformed data in a tabular format.
    """
    transformed_data = []
    headers = []

    # Iterate through predicted fields
    for key, value_ in predicted_fields.items():
        ind = 0
        rang = len(list(value_[0].values())[0])

        # Iterate through the range of values
        for i in range(rang):
            temp = {}
            # Iterate through the values associated with each field
            for value in value_:
                for col, values in value.items():
                    # Add columns to headers if not already present
                    if col not in headers:
                        headers.append(col)
                    try:
                        temp[col] = values[ind]
                    except:
                        temp[col] = ''
            transformed_data.append(temp) 
            ind = ind + 1
            
    final_table = [{"header": headers, "rowData": transformed_data}]
    
    return final_table


#-----------------------------------------------------------------------------------------------------


#each division should be sent to this function along with its relative fileds keywords
def main_2d(ocr_data,field_keywords,predcit,seg):
    """
    Process all the keys and extarct the values for the lables that we have  from the tables

    Args:
        ocr_data (list of dicts): OCR data containing words with their properties.
        field_keywords (dict): Keywords related to fields and headers.
        predcit (str): Prediction context.
        seg (str): Segment identifier.

    Returns:
        tuple: A tuple containing three elements:
            - predict_values (dict): Predicted values extracted from the OCR data.
            - removed_word_map (dict): removed words.
            - custom_table (list): Custom table formed based on the extracted data.
    """
    predict_values={}
    custom_table=[]
    removed_word_map={}
    find_al=[]
    field=field_keywords['field_keywords']
    header=field_keywords['header_keywords']
    table_headers=field_keywords['table_headers']
    ocr_data_=ocr_data
    if field and header:
        print(f"entered the if of main 2d")
        for key in field:
            for data in ocr_data:
                word=data['word']
                if key==word:
                    lable={}
                    next_values=find_next_ele_stan(data,ocr_data)
                    print(F"nexvalues got is {next_values}")
                    try:
                        if next_values:
                            next_values = sorted(next_values, key=lambda x: x["left"])
                    except:
                        pass  
                    for each_value in next_values:

                        if table_headers:
                            table_header=get_head_alias(table_headers,seg)
                        else:
                            table_header=find_table_header(each_value,ocr_data,header,predcit,seg)
                            find_al.append(table_header)
                        print(f"table_header got is {table_header} for each value {each_value['word']}")

                        if table_header:
                            if table_header in lable:
                                lable[table_header].append(each_value['word'])
                            else:
                                lable[table_header]=[each_value['word']]
                        
                        ocr_data_=remove_from_ocr(each_value['word'],ocr_data_)
                    if key in predict_values:
                        if lable not in predict_values[key]:
                            predict_values[key].append(lable)
                    else:
                        predict_values[key]=[lable]
        print("forming custom table")
        custom_table=custom_1(predict_values)

    elif header:
        print(f"entered the else of main 2d")
        for key in header:
            for data in ocr_data:
                word=data['word']
                if key==word:
                    lable={}
                    next_values=find_next_ele_below(data,ocr_data)
                    print(next_values)
                    next_values=filter_num(next_values)
                    print(f"numerical_values got is {next_values}")
                    if next_values:
                        for each in next_values:
                            if key in lable:
                                lable[key].append(each['word'])
                            else:
                                lable[key]=[each['word']]

                        ocr_data_=remove_from_ocr(next_values,ocr_data_)
                        if predcit in predict_values:
                            if lable not in predict_values[predcit]:
                                predict_values[predcit].append(lable)
                        else:
                            predict_values[predcit]=[lable]
        print("forming the custom table")
        custom_table=custom_2(predict_values)
        
    return predict_values,removed_word_map,custom_table


#------------------------------------------------------------------------------------------------
        

def find_next_ele(key, ocr_data):
    """
    Finds the next possible values for a given key.

    Args:
        key: Dictionary containing information about the key.
        ocr_data: List of dictionaries containing OCR data.

    Returns:
        next_values: List of dictionaries containing the next possible values.
    """
    # Initialize an empty list to store the next possible values
    next_values = []
    
    # Sort OCR data based on the 'top' coordinate
    sorted_words = sorted(ocr_data, key=lambda x: x["top"])
    
    # Get the right coordinate of the key
    k_right = key["right"]
    
    # Iterate through sorted words
    for word in sorted_words:
        # Calculate the vertical difference between the word and the key
        diff_ver = abs(word["top"] - key["top"])
        
        # Check if the vertical difference is less than 5 pixels and the word is to the right of the key
        if diff_ver < 5 and k_right < word["left"]:
            # If conditions are met, add the word to the next_values list
            next_values.append(word)
                
    return next_values


#--------------------------------------------------------------------------------------------------

def read_corpus_from_json(json_file):

    #read the data from the jsn file

    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


def find_range(input_value, ranges):

    #get the range of the dates based on the date ranges

    for i in range(len(ranges) - 1):
        if input_value < ranges[i]:
            return [ranges[i]]
        elif input_value >= ranges[-1]:
            return [ranges[-1]]
        elif ranges[i] <= input_value < ranges[i + 1]:
            return [ranges[i],ranges[i+1]]
        

def get_col_range(input_string,row_head=''):
    """
    Parse and standardize column range values.

    Args:
        input_string (str): Input string containing column range information.
        row_head (str): Row header.

    Returns:
        str: Standardized column range string.
    """

    try:
        corpus=[[],[7,30,60,90,120,150,180]]
        low=input_string.lower()
        input_string = re.sub(r'[^-<>0-9]', '', input_string)

        if row_head in corpus[1]:
            ranges=corpus[1][row_head]
        else:
            ranges=corpus[1]
        if '<' in input_string:
            values = [int(val) for val in input_string.split('<') if val.isdigit()]
            if len(values)==1: 
                value__=values[0]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string="<"+str(value__)
                    return input_string
                else:
                    out=find_range(values[0], ranges)
            else:
                value__=values[1]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string="<"+str(value__)
                    return input_string
                else:
                    out=find_range(values[1], ranges)
            print(f"out is {out}")
            if len(out)==1:
                if value__>out[0]:
                    input_string=">"+str(out[0])
                else:
                    input_string="<"+str(out[0])
            else:
                input_string="<"+str(out[1])
        elif '-' in input_string or 'to' in low:
            if '-' in input_string:
                values = [int(val) for val in input_string.split('-') if val.isdigit()]
            else:
                input_string=re.sub(r'[^-<>0-9to]', '', low)
                values = [int(val) for val in input_string.split('to') if val.isdigit()]
            if len(values)==1: 
                value__=values[0]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string="<"+str(value__)
                    return input_string
                else:
                    out=find_range(values[0], ranges)
            else:
                value__=values[1]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string="<"+str(value__)
                    return input_string
                else:
                    out=find_range(values[1], ranges)
            print(f"out is {out}")
            if len(out)==1:
            
                if value__>out[0] or 'above' in input_string:
                    input_string=">"+str(out[0])
                else:
                    input_string="<"+str(out[0])
            else:
                print(value__)
                input_string="<"+str(out[1])
        elif '>' in input_string:
            values = [int(val) for val in input_string.split('>') if val.isdigit()]
            if len(values)==1: 
                value__=values[0]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string=">"+str(value__)
                    return input_string
                else:
                    out=find_range(values[0], ranges)
            else:
                value__=values[1]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string=">"+str(value__)
                    return input_string
                else:
                    out=find_range(values[1], ranges)
            if len(out)==1:
                if value__>out[0]:
                    input_string=">"+str(out[0])
                else:
                    input_string="<"+str(out[0])
            else:
                input_string="<"+str(out[1])
        elif input_string:
            values = [int(input_string)]
            if len(values)==1: 
                value__=values[0]
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string="<"+str(value__)
                    return input_string
                else:
                    out=find_range(values[0], ranges)
                print(out)
            else:
                if value__ in ranges:
                    if 'above' in low:
                        input_string=">"+str(value__)
                    else:
                        input_string="<"+str(value__)
                    return input_string
                else:
                    out=find_range(values[0], ranges)
            if len(out)==1:
                if value__>out[0]:
                    input_string=">"+str(out[0])
                else:
                    input_string="<"+str(out[0])
            else:
                input_string=">"+str(out[0])
            
                            
        print(F"returing is {input_string}")                  
        return input_string                 
    except Exception as e:
        print(f'exceptionis {e}')
        return ''


def get_db_labels(word_list,seg,div):
    model_filename = f'/var/www/extraction_api/app/extraction_folder/{seg}/{div}_logistic_regression_model.joblib'
    vectorizer_filename = f'/var/www/extraction_api/app/extraction_folder/{seg}/{div}_count_vectorizer.joblib'

    try:
        # Load the model
        clf = joblib.load(model_filename)

        # Load the vectorizer
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(F"file not found {vectorizer_filename,model_filename}")
        return ''

    # Convert the input list of words to a bag of words (BoW) representation
    X_inference = vectorizer.transform(word_list)
                     
    # Make predictions using the loaded model
    predictions = clf.predict(X_inference)
    
    output=''
    # print the predictions for each word
    
    for word, prediction in zip(word_list, predictions):
        print(f"Word: {word}, Predicted Label: {prediction}")
        if prediction !='other':
            output=prediction
    return output


def update_into_db(predicted_fields,case_id,tenant_id,seg,pred):
    """
    Update extracted fields into the database.

    Args:
        predicted_fields (dict): Predicted fields and their values.
        case_id (str): Case ID.
        tenant_id (str): Tenant ID.
        seg (str): Segment identifier.
        pred (str): Prediction identifier.

    Returns:
        tuple: A tuple containing three elements:
            - highlight (dict): Dictionary containing highlights for fields.
            - high_map (dict): Mapping of highlighted fields.
            - multi_add (dict): Dictionary containing multi-values fields.
    """
    
    highlight={}
    high_map={}
    multi_add={}
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)

    out={}
    for key,values in predicted_fields.items():
            for value in values:
                for col,val in value.items():

                    column=get_db_labels([key],seg,"col")
                    print(f"get_db_labels column got is {column}")

                    col_head=get_col_range(col)
                    print(f"range got is {col_head}")
                    if column:
                        if not col_head:
                            head=get_db_labels([col],seg,"head")
                            print(f"get_db_labels head got is {head}")
                            if column+"_"+head not in out:
                                out[column+"_"+head]=val[0]
                                highlight[column+"_"+head]=val[0]
                                high_map[column+"_"+head]=[key,col]
                        else:
                            if column+"_"+col_head not in out:
                                out[column+"_"+col_head]=val[0]
                                highlight[column+"_"+col_head]=val[0]
                                high_map[column+"_"+col_head]=[key,col]
                    else:
                        if key+"_"+col not in out:
                            out[key+"_"+col]=val[0]
                            highlight[key+"_"+col]=val[0]
                            high_map[key+"_"+col]=[key,col]
    print(f"out is {out}")
    
    rep=pred.replace(" ","_")
    if '_OCR' not in rep:
        temp=rep+'_OCR'
    else:
        temp=rep
    temp=temp.upper()

    if temp=="DEBTORS_TABLE_OCR":
        temp="DEBTORS"
    # query = f"select `{temp}` from ocr where case_id='{case_id}'"
    # column = extraction_db.execute_(query)[temp].to_list()[0]

    query = f"select `{temp}` from ocr where case_id='{case_id}'"
    column = extraction_db.execute_(query)[temp].to_list()[0]
    if column:

        column=json.loads(column)
        for key,value in out.items():
            column[key]=value

        chunk_size = 4000 
        value=json.dumps(column)
        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {temp} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            sql += "TO_CLOB('" + chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
        
        extraction_db.execute_(sql)
        print(F" ####### final out_dict to store in the db is {column}")
        # query=f"UPDATE `ocr` SET `{temp}` = '{json.dumps(column)}' WHERE `case_id` = '{case_id}'"
        # extraction_db.execute_(query)
    else:

        chunk_size = 4000 
        value=json.dumps(out)
        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {temp} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            sql += "TO_CLOB('" + chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
        
        extraction_db.execute_(sql)
        print(F" ####### final out_dict to store in the db is {predicted_fields}")

    return highlight,high_map,multi_add


def update_db_nf(predicted_fields,case_id,tenant_id,seg):

    #updating non table fields that extarcted into database
    
    db_config['tenant_id'] = tenant_id
    json_file_path = f"/var/www/extraction_api/app/extraction_folder/{seg}/{seg}_db.json"
    corpus = read_corpus_from_json(json_file_path)[0]
#     print(corpus)
    extraction_db = DB('extraction', **db_config)
    for key,value in predicted_fields.items():
        cleaned_string = re.sub(r'[^a-zA-Z0-9]', '', key)
        cleaned_string = cleaned_string.lower()
        print(f"cleaned_string are {cleaned_string} ")
        if cleaned_string in corpus:
            
            column=(corpus[cleaned_string])
            query=f"UPDATE `ocr` SET `{column}` = '{value}' WHERE `case_id` = '{case_id}'"
            extraction_db.execute_(query)
    
    return ''


def combine_dicts(dicts):

    #this function is used for combining ocr dicts into one

    combined_dict = {
        "height": max([d["height"] for d in dicts]),
        "top": min([d["top"] for d in dicts]),
        "left": min([d["left"] for d in dicts]),
        "bottom": max([d["bottom"] for d in dicts]),
        "right": max([d["right"] for d in dicts]),
        "width": abs(max([d["right"] for d in dicts])-min([d["left"] for d in dicts])),
        "confidence": max([d["confidence"] for d in dicts]),
        "sen_no": dicts[0]["sen_no"],  # Assuming they all have the same sen_no
        "pg_no": dicts[0]["pg_no"],# Assuming they all have the same pg_no
        "x-space":dicts[0]["x-space"]
    }
    return combined_dict


def get_highlight_t(update_ocr_data,high_map,start_page,ocr_data_word,predcit,removed_word_map,multi_add):

    """
    Get highlights for extracted data.

    Args:
        update_ocr_data (dict): predicted fields.
        high_map (dict): Mapping of highlighted fields.
        start_page (int): Starting page of the document.
        ocr_data_word (list): OCR data words.
        predcit (str): predicted table.
        removed_word_map (dict): Removed word mapping.
        multi_add (dict): Multi-value fields.

    Returns:
        dict: Dictionary containing highlights for table data.
    """

    print(f"data got is {update_ocr_data , high_map , start_page  , predcit , removed_word_map , multi_add}")
    print(f"removed_word_map got is {removed_word_map}")
    value_highlights={}
    try:
        for key,value_ in update_ocr_data.items():
            if key in multi_add:
                values=multi_add[key]
            else:
                values=[value_]
            print(F"values is {values}")
            for value__ in values:
                temp={}
                for words in ocr_data_word:
                    value=value__
                    if value==words['word']:
                        print(f"word for which highlighit needed  is {value}")
                        row_head=high_map[key][1]
                        col_head=high_map[key][0]
                        print(f"word for which row_head col_head  is{row_head} and {col_head}")
                        out=find_table_header(words,ocr_data_word,[row_head],[])
                        before_list=find_before_ele(words,ocr_data_word)
                        print(f"row head got is {out} and column head got is {before_list}")
                        if (out and col_head in before_list) or predcit==col_head:
                            temp['word']=value
                            temp['top']=words['top']
                            temp['left']=words['left']
                            temp['bottom']=words['bottom']
                            temp['right']=words['right']
                            temp['page']=start_page
                            temp['x']= temp['left']
                            temp['y']=temp['top']

                            max_height = temp['bottom'] - temp['top']
                            total_width = temp['right'] - temp['left']
                            temp['height']=max_height
                            temp['width']=total_width
                            break
            value_highlights[key]=temp
            
            
        print(f"value_highlights is {value_highlights}")
    except Exception as e:
        print(f"error occured in fn get_highlight_t error is {e}")
        pass

    return value_highlights


def get_highlight_n_t(update_ocr_data,start_page,ocr_data_word,seg):

    """
    Extract highlights for non-table data.

    Args:
        update_ocr_data (dict): predicted fields.
        start_page (int): Starting page of the document.
        ocr_data_word (list): OCR data words.
        seg (str): Segment identifier.

    Returns:
        dict: Dictionary containing highlights for non-table data.
    """

    print(f"data got is {update_ocr_data}")
    try:

        value_highlights={}

        json_file_path = f"/var/www/extraction_api/app/extraction_folder/{seg}/{seg}_db.json"
        corpus = read_corpus_from_json(json_file_path)[0]

        for key,values in update_ocr_data.items():
            cleaned_string = re.sub(r'[^<>a-zA-Z0-9]', '', key)
            cleaned_string = cleaned_string.lower()
            for words in ocr_data_word:
                temp={}
                if values==words['word']:
                    temp['word']=values
                    temp['top']=words['top']
                    temp['left']=words['left']
                    temp['bottom']=words['bottom']
                    temp['right']=words['right']
                    temp['page']=start_page
                    temp['x']= words['left']
                    temp['y']=words['top']

                    max_height = temp['bottom'] - temp['top']
                    total_width = temp['right'] - temp['left']
                    temp['height']=max_height
                    temp['width']=total_width

                    if cleaned_string in corpus:
                        cleaned_string=corpus[cleaned_string]
                    if cleaned_string not in value_highlights:
                        value_highlights[cleaned_string]=temp
                    
        print(f"value_highlights is {value_highlights}")
            
        return value_highlights
    except Exception as e:
        print(f"error occured in fn get_highlight_n_t error is {e}")
        return {}
    

def update_highlights(tenant_id,case_id,highlights):

    #updating all the extracted highlights into database

    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    query=f"select * from ocr where case_id='{case_id}'"
    highlight_df=extraction_db.execute(query)
    highlight=highlight_df.to_dict(orient='records')
    highlight=highlight[0]['highlight']

    if highlight:
        highlight=json.loads(highlight)
        #highlight.update({key: value for key, value in highlights.items() if key not in highlight})
        highlight.update(highlights)
        highlight=json.dumps(highlight)
        query = "UPDATE `ocr` SET `highlight`= %s WHERE `case_id` = %s"
        params = [highlight, case_id]
        extraction_db.execute(query, params=params)
    else:
        word_highlight=json.dumps(highlights)
        query = "UPDATE `ocr` SET `highlight`= %s WHERE `case_id` = %s"
        params = [word_highlight, case_id]
        extraction_db.execute(query, params=params)
    return "true"


def find_table_highlight(ocr_data,row_head,col_head,value,start_page):

    print(f"data got is {row_head , col_head , value ,start_page}")


    """
    Find table highlight.

    Args:
        ocr_data (list): OCR data.
        row_head (str): Row header.
        col_head (str): Column header.
        value (str): Value to find highlight for.
        start_page (int): Starting page of the document.

    Returns:
        dict: Dictionary containing table highlight information.
    """

    ret_out={}
    for word in ocr_data:
        if word["word"]==value:
            out=find_table_header(word,ocr_data,[row_head],'','',api=False)
            temp={}
            before_list=find_before_ele(word,ocr_data)
            if (out and col_head in before_list):
                temp['page']=start_page
                max_height = word['bottom'] - word['top']
                total_width = word['right'] - word['left']
                temp['height']=max_height
                temp['width']=total_width
                temp['x']= word['left']
                temp['y']=word['top']
                ret_out['boundary']=temp
                ret_out['text']=word['word']
                word['page']=start_page
                ret_out.update(word)
    return ret_out


def get_table_high(ocr_data,data,seg,start_page):

    """
    Get table highlight.

    Args:
        ocr_data (list): OCR data.
        data (list): Data extracted from the table.
        seg (str): Segment name.
        start_page (int): Starting page of the document.

    Returns:
        dict: Dictionary containing table highlight information.
    """

    row_data=data[0]["rowData"]
    header=data[0]["header"]
    high_table={}
    dit=[]
    for row in row_data:
        temp={}
        for row_head in header:
            col_head=row['Particulars']
            if row_head!='Particulars':
                if row_head in row:
                    out=find_table_highlight(ocr_data,row_head,col_head,row[row_head],start_page)
                    temp[str(row_head)]=out
                else:
                    temp[row_head]={}
        dit.append(temp)
    high_table[seg+'_data']=dit
        
    return high_table


#--------------------------------------------------------------------------------------------------


@app.route("/extraction_2d_table", methods=['POST', 'GET'])
def extraction_2d_table():

    """
    Perform extraction of tables.

    Returns:
        dict:Response containing extraction status.
    """

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    print(f"Data recieved: {data}")
    try:
        tenant_id = data['tenant_id']
        try:
            case_id= data['email']['case_id']
        except:
            case_id = data.get('case_id', None)

        user = data.get('user', None)
        session_id = data.get('session_id', None)
    except Exception as e:
        logging.warning(f'## TE Received unknown data. [{data}] [{e}]')
        return {'flag': False, 'message': 'Incorrect Data in request'}
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    extraction_db = DB('extraction', **db_config)
    query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
    document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
    document_id_df_all=json.loads(document_id_df_all)
    response={}
    # insert_data = {
    #         'CASE_ID': case_id
    #     }
    # extraction_db.insert_dict(insert_data, 'custom_table')

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
        service_name='extraction_api',
        span_name='extraction_2d',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5, ):

        try:

            query = f"SELECT `seg` from  `ocr` where `case_id` = '{case_id}'"
            seg = extraction_db.execute_(query)['seg'].to_list()[0]
            print(f"seg got is {seg}")

            updated=False
            non_up=False
            start_page=0
            try:
                print(F" ################ ")
                print(f"F ################ ")
                print(f"F ################ strating the prediction for non_tabular fields")
                print(f"F ################ ")
                print(F" ################ ")
                if start_page==0:

                    value_predict_params={
                        "case_id":case_id,
                        "document_id":case_id,
                        "field_prediction_centroid_flag":True,
                        "start_page":start_page,
                        "end_page":start_page,
                        "ui_train_info":{},
                        "template_name":'',
                        "seg":seg,
                        "tenant_id":tenant_id,
                        "file_type":"non_table_fields"
                    }
                    host = 'predictionapi'
                    port = 443
                    route = 'cetroid_api'
                    logging.debug(f'Hitting URL: https://{host}:{port}/{route} for template non_table_fields')
                    # logging.debug(f'Sending Data: {value_predict_params}')
                    headers = {'Content-type': 'application/json; charset=utf-8',
                            'Accept': 'text/json'}
                    response_ = requests.post(f'https://{host}:{port}/{route}', json=value_predict_params, stream=True,verify=False)
                    
                    response_=response_.json()

                    print(f" ####### Response Received from cetroid_api is {response_}")

                    
                    # non_fields=predict_non_table_fields(word_list,seg)
                    # n_t_v_highlights=get_highlight_n_t(non_fields,start_page,document_id_df,seg)
                    # update_highlights(tenant_id,case_id,n_t_v_highlights)
                    # print(f"non_table_fields  got is {non_fields}")

                    
                    # update_db_nf(non_fields,case_id,tenant_id,seg)
            except Exception as e:
                logging.exception(e)
                print(f"########## error in exttraction of non_table fields")

            if seg:
                for document_id_df in document_id_df_all:
                    word_list=[]
                    for word in document_id_df:
                        word_list.append(word['word'])

                    print(f"word_list got is {word_list}")
                    
                    print(f"strating the table_fields prediction \n")
                    if word_list:
                        divisions=predict_table_divisions(document_id_df,word_list,seg)
                    print(f"divisions recived from predict_table_divisions {divisions}\n")

                    query = f"select table_div from `required_fields_table` where seg='{seg}'"
                    req_list = extraction_db.execute_(query)['table_div'].to_list()

                    for predcit,word_div__ in divisions.items(): 
                        for word_div_ in word_div__:
                            if not word_div_:
                                continue
                            print(f"the prediction staring for divsion is {predcit} \n")

                            print(f"divisions send to enhance_divisions_ver is {word_div_} \n")

                            print(f"divisions send to enhance_divisions_ver is {word_div_} \n")
                            # word_div_=enhance_divisions_ver(word_div_,predcit,seg)
                            word_div_=[word_div_]
                            print(f"divisions got from enhance_divisions_ver is {word_div_} \n")
                        

                            for word_div in word_div_:
                                word_list=[]
                                for word in word_div:
                                    word_list.append(word['word'])

                                field_keywords=predict_table_headers(word_list,predcit,seg)
                                print(f"field_keywords got from predict_table_headers is {field_keywords} \n")

                                if field_keywords:

                                    if field_keywords['header_keywords']:
                                        print(f" headers sent to table header check is {field_keywords['header_keywords'],word_div} \n")
                                        word_div,field_keywords['header_keywords']=check_headers(field_keywords['header_keywords'],word_div)
                                        print(f"headers got from table header check is {word_div}  and {field_keywords['header_keywords']}\n")
                                    if field_keywords['field_keywords'] and field_keywords['header_keywords'] :
                                        field_keywords,sort_list=enhance_headers(field_keywords,word_div)
                                        print(f"sort_list is {sort_list}\n")
                                    else:
                                        sort_list={}

                                    print(f"field_keywords got from enhance_headers is {field_keywords}\n")
                                    predicted_fields,removed_word_map,final_table=main_2d(word_div,field_keywords,predcit,seg)
                                    
                                    print(f"output from predicted_fields is {predicted_fields}\n")
                                    q_pred=predcit.replace(" ","_")
                                    query = f"select `{q_pred}` from custom_table where case_id='{case_id}'"
                                    query_data = extraction_db.execute_(query)
                                    try:
                                        column=list(query_data[predcit])[0]
                                    except Exception:
                                        column=[]

                                    try:
                                        if predicted_fields:
                                            response.update(predicted_fields)
                                            updated=True
                                            non_up=True
                                            try:    
                                                table_high=get_table_high(word_div,final_table,predcit,start_page)
                                                print(F"table highlight got is {table_high}")
                                            except:
                                                table_high={}
                                                pass

                                            # if not column:
                                            #     query=f"UPDATE `custom_table` SET `{q_pred}` = '{json.dumps(final_table)}' WHERE `case_id` = '{case_id}'"
                                            #     extraction_db.execute_(query)

                                            chunk_size = 4000 
                                            final_table=json.dumps(final_table)
                                            chunks = [final_table[i:i+chunk_size] for i in range(0, len(final_table), chunk_size)]

                                            # Build the SQL query to insert chunks into the CLOB column
                                            sql = f"INSERT INTO custom_table (case_id,{q_pred}) VALUES ('{case_id}',"

                                            # Append each chunk to the SQL query
                                            for chunk in chunks:
                                                sql += "TO_CLOB('" + chunk + "') || "

                                            # Remove the last ' || ' from the SQL query
                                            sql = sql[:-4] + ")"

                                            extraction_db.execute_(sql)


                                            high_,high_map,multi_add=update_into_db(predicted_fields,case_id,tenant_id,seg,predcit)


                                            print(F"highlight got is {high_} and map is {high_map}")
                                            if high_:
                                                highlights=get_highlight_t(high_,high_map,start_page,word_div,predcit,removed_word_map,multi_add)
                                                highlights.update(table_high)
                                                update_highlights(tenant_id,case_id,highlights)
                                            else:
                                                update_highlights(tenant_id,case_id,table_high
                                                                  )
                                            try:
                                                req_list.remove(predcit)
                                            except:
                                                pass 


                                    except Exception as e:
                                        logging.exception(e)
                                        print(f"########## unable to upadate into db")
                        
                    start_page=start_page+1
            if not updated or not seg:
                print(F" ################ ")
                print(F" ################ ")
                print(f" ################ starting non standard predcition ")
                print(F" ################ ")
                print(F" ################ ")
                response_non_stand=non_standard_docs(case_id,tenant_id,seg)
                print(F" ################ ")
                print(F" ################ ")
                print(f" ################ done with non standard predcition ")
                print(F" ################ ")
                print(F" ################ ")
                
                response.update(response_non_stand)
            elif non_up:
                for remain in req_list:
                    try:
                        update_into_db({},case_id,tenant_id,seg,remain)
                    except Exception as e:
                        print(F" ############## is {e}")
                update_db_ntable({}, case_id, tenant_id)
            
            response_data={"flag":True,"data":{"message":" 2d extraction api sucess."}}
            print(f"## TE info func_name : extract_main \n final_output response_data is  : {response_data} \n")

            print("checking extraction api status")

        except Exception as e:
            logging.debug("Something went wrong!")
            logging.exception(e)
            response_data={"flag":False,"data":{"message":" 2d extraction api failed."}}


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
                        "api_service": "extraction_2d", "service_container": "extraction_api",
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(response), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(case_id, audit_data)

    return response_data


#--------------------------------------------------------------------------------------------------


@app.route("/custom_table_data", methods=['POST', 'GET'])
def custom_table_data():
    """ This route is kotak purpsoe
        This route makes individual columns data from ocr table into a custom table format to view on UI
    """
    data = request.json
    print(f"requested data {data}")
    tenant_id = data.get('tenant_id', None)

    db_config["tenant_id"]=tenant_id
    case_id = data.get('case_id', None)
    extraction_db = DB("extraction",**db_config)
    
    try:
         
        consumer_stock_details = f"SELECT  case_id , co_rm_lt30, co_rm_6to60, co_rm_30to90, co_rm_lt60, co_rm_60to90, co_rm_90, co_rm_90to120, co_rm_90to180, co_rm_lt120, co_rm_120to150, co_rm_120to180, co_rm_gt180, co_rm_unit_quan,co__wip_lt30, co_wip_6to60, co_wip_30to90, co_wip_lt60, co_wip_60to90, co_wip_lt90, co_wip_90to120, co_wip_90to180, co_wip_lt120, co_wip_120to150, co_wip_120to180, co_wip_gt180, co_wip_unit_quan,`co_con&pm_lt30`, `co_con&pm_6to60`, `co_con&pm_30to90`, `co_con&pm_lt60`, `co_con&pm_60to90`, `co_con&pm_lt90`, `co_con&pm_90to120`, `co_con&pm_90to180`, `co_con&pm_lt120`, `co_con&pm_120to150`, `co_con&pm_120to180`, `co_con&pm_gt180`, `co_con&pm_unit_quan`,co_fg_lt30, co_fg_6to60, co_fg_30to90, co_fg_lt60, co_fg_60to90, co_fg_lt90, co_fg_90to120, co_fg_90to180, co_fg_lt120, co_fg_120to150, co_fg_120to180, co_fg_gt180, co_fg_unit_quan,co_vs_lt30, co_vs_6to60, co_vs_30to90, co_vs_lt60, co_vs_60to90, co_vs_lt90, co_vs_90to120, co_vs_90to180, co_vs_lt120, co_vs_120to150, co_vs_120to180, co_vs_gt180, co_vs_unit_quan,co_ss_lt30, co_ss_6to60, co_ss_30to90, co_ss_lt60, co_ss_60to90, co_ss_lt90, co_ss_90to120, co_ss_90to180, co_ss_lt120, co_ss_120to150, co_ss_120to180, co_ss_gt180, co_ss_unit_quan,co_total_stock_lt30, co_total_stock_6to60, co_total_stock_30to90, co_total_stock_lt60, co_total_stock_60to90, co_total_stock_lt90, co_total_stock_90to120, co_total_stock_90to180, co_total_stock_lt120, co_total_stock_120to150, co_total_stock_120to180, co_total_stock_gt180, co_total_stock_unit_quan from ocr where case_id='{case_id}';"
        consumer_stock_details_data = extraction_db.execute_(consumer_stock_details) 
        co_stock_header = consumer_stock_details_data.columns.tolist()
        co_stock_rowdata = consumer_stock_details_data.to_dict(orient='records')
        consumer_stock_details_tabledata = [{"header":co_stock_header,"rowData":co_stock_rowdata}]

        consumer_debtors_details = f"SELECT case_id, co_3rdparty_debtors_lt30,    co_3rdparty_debtors_30to60,    co_3rdparty_debtors_6to60,    co_3rdparty_debtors_lt60,    co_3rdparty_debtors_60to90,    co_3rdparty_debtors_lt90,    co_3rdparty_debtors_90to120,    co_3rdparty_debtors_120to180,    co_3rdparty_debtors_90to180,    co_3rdparty_debtors_gt180,    co_3rdparty_noof_debtors, co_spare_debtors_lt30,    co_spare_debtors_30to60,    co_spare_debtors_6to60,    co_spare_debtors_lt60,    co_spare_debtors_60to90,    co_spare_debtors_lt90,    co_spare_debtors_90to120,    co_spare_debtors_120to180,    co_spare_debtors_90to180,    co_spare_debtors_gt180,    co_spare_debtors_noof_debtors,co_vehicle_debtors_lt30,    co_vehicle_debtors_30to60,    co_vehicle_debtors_6to60,    co_vehicle_debtors_lt60,    co_vehicle_debtors_60to90,    co_vehicle_debtors_lt90,    co_vehicle_debtors_90to120,    co_vehicle_debtors_120to180,    co_vehicle_debtors_90to180,    co_vehicle_debtors_gt180,    co_vehicle_debtors_noof_debtors from ocr where case_id='{case_id}';"
        consumer_debtors_details_data = extraction_db.execute_(consumer_debtors_details)
        co_debotrs_header = consumer_debtors_details_data.columns.tolist()
        co_debotrs_rowdata = consumer_debtors_details_data.to_dict(orient='records')
        consumer_debtors_details_tabledata = [{"header":co_debotrs_header,"rowData":co_debotrs_rowdata}]

        consumer_creditors_details = f"SELECT case_id , co_lcbc_amount, co_lcbc_noof_creditors ,co_otherthan_lcbc_amount, co_otherthan_lcbc_nof_cred ,co_spares_creditors_amount, co_spares_nof_cred,co_total_creditors_amount, co_total_credi_nof_credit from ocr where case_id='{case_id}';"
        consumer_creditors_details_data = extraction_db.execute_(consumer_creditors_details)
        co_creditors_header = consumer_creditors_details_data.columns.tolist()
        co_creditors_rowdata = consumer_creditors_details_data.to_dict(orient='records')
        consumer_creditors_details_tabledata = [{"header":co_creditors_header,"rowData":co_creditors_rowdata}]

        insert_data = {
            'cons_stock_table': json.dumps(consumer_stock_details_tabledata), 
            'cons_debtors_table': json.dumps(consumer_debtors_details_tabledata),
            'cons_credi_table': json.dumps(consumer_creditors_details_tabledata),
            'case_id': case_id
        }
        extraction_db.insert_dict(insert_data, 'custom_table')
        return {"flag":True,"data":{"message":"custom tables are updated"}}
    except Exception as e:
        logging.exception("Error in custom_table function", e)
        return {"flag":False,"message":"custom tables are not updated"}


def update_into_db_table(predicted_fields,tenant_id,case_id,seg,pred):
    
    highlight={}
    out_dict={}
    multi_add={}
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    for key,values in predicted_fields.items():
        for col,val in values.items():
                print(f"get_db_labels sent is {key}")
                column=get_db_labels([key],seg,"column")
                print(f"get_db_labels column got is {column}")
                key_ = re.sub(r'[^<>a-zA-Z0-9]', '', pred+column)
                key_= key_.lower()
                print(f" range sent to col_finder {key_,col}")
                col_head=get_col_range(col,key_,seg,False)
                print(f"range got is {col_head}")
                if col_head:
                    head=get_db_labels([col_head],seg,"head")
                else:
                    head=get_db_labels([col],seg,"head")
                print(f"get_db_labels head got is {head}")
                if isinstance(val, list):
                    val= ','.join(map(str, val))
                else:
                    val=val
                if column and head:
                    if head=='DATE' or head=='Date':
                        print(F"date")
                        if (column+"_"+col_head) in multi_add and val:
                            multi_add[column+"_"+col_head].append(val)
                        else:
                            if val:
                                multi_add[column+"_"+col_head]=[val]
                        if col_head:
                            out_dict[column+"_"+col_head]=val
                            highlight[column+"_"+col_head]=val
                        else:
                            out_dict[column+"_"+col]=val
                            highlight[column+"_"+col]=val
                    else:
                        out_dict[column+"_"+head]=val
                        highlight[column+"_"+head]=val
                    
                print(F"\n")
    print(F" #######    out_dict to before sum is {out_dict} and {highlight}")
    print(F" #######    multi_add got is  {multi_add}")
    remove=[]
    for key,values in multi_add.items():
        if len(values)==1:
            remove.append(key)
            continue
        total_sum=0
        for value in values:
            temp=float(re.sub(r'[^0-9.]', '', value))
            total_sum = total_sum+temp
        if key in out_dict:
            out_dict[key]=str(total_sum)
    for k in remove:
        multi_add.pop(k)

    print(F" #######    out_dict to store in the db is {out_dict} and {highlight}")
    
    print(F" #######    out_dict to store in the db is {out_dict}")
    temp=pred+'_OCR'
    print(F" ####### final out_dict to store in the db is {out_dict}")

    try:
        field_accuracy_(out_dict,tenant_id,case_id,temp)
    except Exception as e:
        logging.exception("Error in custom_table function", e)
        pass


    query = f"select fields from `required_fields_table` where seg='{seg}' and table_div='{pred}'"
    query_data = extraction_db.execute_(query)['fields'].to_list()[0]
    if query_data:
        for field_list in json.loads(query_data):
            temp=0
            for field in field_list:
                if field in out_dict:
                    print(field_list[0],field,out_dict)
                    out_dict[field_list[0]]=out_dict[field]
                    if field!=field_list[0]:
                        out_dict.pop(field)
                    temp=1
            if temp!=1:
                out_dict[field_list[0]]=''

    print(F" ####### after addition of missing cols out_dict to store in the db is {out_dict}")

    temp=pred+'_OCR'
    query = f"select `{temp}` from ocr where case_id='{case_id}'"
    column = extraction_db.execute_(query)[temp].to_list()[0]

    if column:

        column=json.loads(column)
        for key,value in out_dict.items():
            column[key]=value

        chunk_size = 4000 
        value=json.dumps(column)
        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {temp} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            sql += "TO_CLOB('" + chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
        
        extraction_db.execute_(sql)
        print(F" ####### final out_dict to store in the db is {column}")
        # query=f"UPDATE `ocr` SET `{temp}` = '{json.dumps(column)}' WHERE `case_id` = '{case_id}'"
        # extraction_db.execute_(query)
    else:

        chunk_size = 4000 
        value=json.dumps(out_dict)
        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {temp} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            sql += "TO_CLOB('" + chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
        
        extraction_db.execute_(sql)
        print(F" ####### final out_dict to store in the db is {out_dict}")
        # query=f"UPDATE `ocr` SET `{temp}` = '{json.dumps(out_dict)}' WHERE `case_id` = '{case_id}'"
        # extraction_db.execute_(query)

    return highlight


def count_different_pairs(ui_dict1, db_dict2):
    changed={}
    not_ext=0
    for key in ui_dict1.keys():
        if key not in db_dict2 and ui_dict1[key]:
            temp={}
            temp['changed_value']=ui_dict1[key]
            temp['actual_value']=''
            changed[key]= temp
            not_ext+=1
        elif key in db_dict2 and ui_dict1[key] != db_dict2[key]:
            if not db_dict2[key]:
                temp={}
                temp['changed_value']=ui_dict1[key]
                temp['actual_value']=db_dict2[key]
                changed[key]= temp
                not_ext+=1
            else:
                temp={}
                temp['changed_value']=ui_dict1[key]
                temp['actual_value']=db_dict2[key]
                changed[key]= temp
    return changed,not_ext

def field_accuracy_(ui_dict,tenant_id,case_id,column):
    changed_all={}
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    queue_db = DB('queues', **db_config)
    not_extracted_all=0
    query = f"select `{column}` from `ocr` where case_id ='{case_id}'"
    query_data = extraction_db.execute_(query)[column].to_list()
    if query_data:
        extracted=json.loads(query_data[0])
        changed,not_ext=count_different_pairs(ui_dict, extracted)
        not_extracted_all+=not_ext
        changed_all.update(changed)
        print(f"changed are {changed_all}") 

    query = f"SELECT * from `field_accuracy` WHERE case_id='{case_id}'"
    field_accuracy = queue_db.execute_(query).to_dict(orient='records')[0]
    if field_accuracy:
        extarcted=int(field_accuracy['total_fields_extracted'])
        if field_accuracy['fields_changed']:
            fields_changed = json.loads(field_accuracy['fields_changed'])
            fields_changed.update(changed_all)
            changed=len(fields_changed)
            total_fileds=int(field_accuracy['total_fields'])+not_extracted_all
            accuracy = (extarcted-changed) / (total_fileds) * 100
            query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(fields_changed)}', `fields_modified`= '{len(fields_changed)}',`percentage`='{accuracy}' WHERE case_id='{case_id}'"
            queue_db.execute(query)
        else:
            changed=len(changed_all)
            total_fileds=int(field_accuracy['total_fields'])+not_extracted_all
            accuracy = ((extarcted-changed) / (total_fileds)) * 100
            query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(changed_all)}', `fields_modified`= '{len(changed_all)}' ,`percentage`='{accuracy}' WHERE case_id='{case_id}'"
            queue_db.execute(query)

    return ''


@app.route("/save_table_ocr", methods=['POST', 'GET'])
def save_table_ocr():
    data = request.json
    print(f"requested data {data}")
    tenant_id = data.get('tenant_id', None)

    db_config["tenant_id"]=tenant_id
    extraction_db = DB('extraction', **db_config)
    case_id = data.get('case_id', None)
    ui_data=data.get('ui_data',{})
    fields=ui_data.get('fields',{})
    field_changes = ui_data.get('field_changes', [])
    variables=ui_data.get('variables',{})
    
    # tables=variables.get('tables',[])
    # tables=json.loads(tables)
    tables=["STOCK STATEMENT","DEBITORS STATEMENT","CREDITORS"]
    seg=data.get('segment','')

    if not seg:
        div = fields.get('division', None)
        query=f"SELECT `segment` FROM `email_checking` where division='{div}'"
        seg=extraction_db.execute_(query)['segment'].to_list()[0]
    
   

    try:
        table=''
        table_val={}
        if 'table' in data:
            table=data.get("c_dict",'')
            if table:
                query = f"SELECT `{data['c_dict']}` FROM `custom_table` WHERE case_id = %s"
                params = [case_id]
                df = extraction_db.execute_(query, params=params)
                df=df[table][0]
                table_val=json.loads(df)
                seg='Consumer'
        else:
            for table_ in field_changes:
                print(table_)
                if table_ in fields and table_ in tables:
                    table= table_
                    table_val=json.loads(fields[table])
        print(f" ############ table_data is {table_val} and table is {table}")
        try:
            if table_val:
                table_dict = {}
                header = table_val[0]["header"]
                for row in table_val[0]["rowData"]:
                    row_dict = {}
                    for i, key in enumerate(header):
                        if key != "Particulars":
                            row_dict[key] = row.get(key, ["-"])
                    table_dict[row["Particulars"]] = row_dict

                print(f" ############ table_dict is {table_dict}")

                if table and table_dict:
                    high_light=update_into_db_table(table_dict,tenant_id,case_id,seg,table)
                else:
                    print(f" ############ table_dict not updated table_dict is {table_dict} table is {table}")
        except Exception as e:
            logging.exception("Error in custom_table function", e)
            pass

        return {"flag":True,"data":{"message":"custom tables are updated"}}

    except Exception as e:
        logging.exception("Error in custom_table function", e)
        return {"flag":True,"message":"custom tables are not updated"}

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port Number', default=5099)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')

    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, port=port, debug=False)
