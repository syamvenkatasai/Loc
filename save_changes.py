
import json
import os
import requests
import ast
import re
import math
import psutil

from app.db_utils import DB
from time import time as tt
from ace_logger import Logging
from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
from app import app
# app = Flask(__name__)

logging = Logging(name='save_changes')

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

def update_table(db, case_id, file_name,data):
    logging.info('Updating table...')

    query = f"SELECT `fields_changed` from `field_accuracy` WHERE case_id='{case_id}'"
    fields_json_string_df = db.execute_(query)
    if not fields_json_string_df.empty:
        update_field_accuracy(
            fields_json_string_df,case_id, db,data
        )
    else:
        insert_field_accuracy(case_id, file_name, db,data)
    return "UPDATED TABLE"


def insert_field_accuracy(case_id, file_name, db,data):
    extraction_db = DB('extraction', **db_config)
    fields=data['fields']
    fields_changed=data['field_changes']
    field_changes={}
    for field in fields_changed:
        temp={}
        temp_main={}
        value_changed=fields[field]
        temp['changed_value']=value_changed
        query=f"select `{field}` from `ocr` where case_id='{case_id}'"
        actual_value=extraction_db.execute_(query)[field].to_list()[0]
        temp['actual_value']=actual_value
        temp_main[field]=temp
        field_changes.update(temp_main)
    total_fields_count=len(fields)
    logging.info(f"count of total fields are {total_fields_count}")
    percentage = len(fields_changed)/total_fields_count
    logging.info(f"percentage calculated {percentage}")
    query = f"INSERT INTO `field_accuracy` (`id`, `case_id`, `file_name`, `fields_changed`, `percentage`) VALUES (NULL,'{case_id}','{file_name}','{json.dumps(field_changes)}','{percentage}')"
    db.execute(query)
    logging.info(f"Inserted into field accuracy for case `{case_id}`")


def update_field_accuracy(fields_json_string_df,case_id,db, data):
    logging.info(f"in update field accuracy function")
    fields_json_string = fields_json_string_df['fields_changed'][0]
    fields_json = json.loads(fields_json_string)
    logging.info(f"fields changed are {fields_json}")
    extraction_db = DB('extraction', **db_config)
    fields=data['fields']
    fields_changed=data['field_changes']
    for field in fields_changed:
        temp={}
        temp_main={}
        value_changed=fields[field]
        temp['changed_value']=value_changed
        query=f"select `{field}` from `ocr` where case_id='{case_id}'"
        actual_value=extraction_db.execute_(query)[field].to_list()[0]
        temp['actual_value']=actual_value
        temp_main[field]=temp
        fields_json.update(temp_main)

    total_fields_count=len(fields)
    logging.info(f"count of total fields are {total_fields_count}")
    fields_json=fields_json
    logging.debug(f"Fields JSON after: {fields_json}")
    percentage = len(fields_json)/total_fields_count
    logging.info(f"percentage calculated for {case_id} is {percentage}")
    query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(fields_json)}', `percentage`= '{percentage}'  WHERE case_id='{case_id}'"
    db.execute(query)
    logging.info(f"Updated field accuracy table for case `{case_id}`")


def dict_split_wise_table(orgi_dict):
    '''
    changes:- added new function for save changes to work on multiple tables
    author:- Amara Sai Krishna Kumar
    '''
    orgi_dict = orgi_dict
    tx_master_db = DB('tx_master', **db_config)

    query = f"select * from master_upload_tables where purpose != ''"
    master_upload_tables_df = tx_master_db.execute_(query)
    l=[]
    parent_dict = orgi_dict
    for each in master_upload_tables_df.index:
        total_table_name = master_upload_tables_df['table_name'][each]
        purpose = master_upload_tables_df['purpose'][each]       
        logging.info(f"##orgi_dict is {orgi_dict}")
        logging.info(f"total_table_name: {total_table_name}, purpose:{purpose}")
        temp={}
        temp1={}
        for k,v in orgi_dict.items():
            test = k.split('_')[0]
            if test.lower() == purpose.lower():
                temp[k]=v
            else:
                continue
            temp1[total_table_name] = temp
        l.append(temp1)
    
    for i in range(len(l)):
        for k,v in l[i].items():
            child_dict = v
            for key in child_dict.keys():
                if key in parent_dict:
                    parent_dict.pop(key)
    l.append({'extraction.ocr':parent_dict})
    logging.info(f"fields are  {l}")
    return l


def count_different_pairs(ui_dict1, db_dict2):
    changed={}
    not_ext=0
    for key in ui_dict1.keys():
        if key not in db_dict2:
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

def field_accuracy_(ui_data,tenant_id,case_id):
    fields=ui_data['fields']
    changes_fields=ui_data['field_changes']
    changed_all={}
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    queue_db = DB('queues', **db_config)
    not_extracted_all=0
    #these are hard codded for now need to create a db table and get it from there 
    custom_data=['STOCK STATEMENT_OCR','DEBITORS STATEMENT_OCR','CREDITORS_OCR','SECURITY DEPOSIT_OCR','ADVANCES SUPPLIERS_OCR','BANK OS_OCR','ADVANCES DEBTORS_OCR','SALES_OCR','ADVANCES_OCR','PURCHASES_OCR']
    other_fields=['date','customer_name']
    for key,value in fields.items():
        if key in custom_data and key in changes_fields:
            query = f"select `{key}` from `ocr` where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)[key].to_list()
            if query_data:
                value=json.loads(value)
                extracted=json.loads(query_data[0])
                changed,not_ext=count_different_pairs(value, extracted)
                not_extracted_all+=not_ext
                changed_all.update(changed)
                logging.info(f"changed fields are {changed_all}")
        elif key in changes_fields and key in other_fields:
            query = f"select `{key}` from `ocr` where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)[key].to_list()
            if query_data:
                extracted=query_data[0]
                if extracted!=value:
                    temp={}
                    temp['changed_value']=value
                    temp['actual_value']=extracted
                    changed_all[key]= temp
                else:
                    temp={}
                    temp['changed_value']=value
                    temp['actual_value']=extracted
                    changed_all[key]= temp

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

def compare_dicts(dict1, dict2):
    if set(dict1.keys()) != set(dict2.keys()):
        return False
    for key in dict1:
        if dict1[key] != dict2[key]:
            return False
    return True

def calculate_distance(point1, point2):
    # print(point1,point2)
    x1, y1 = point1['left'], point1['top']
    x2, y2 = point2['left'], point2['top']
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return distance

def calculate_value_distance(point1, point2):
    # print(point1,point2)
    x1 = (point1['right']-point1['left'])/2
    x2= (point2['right']- point2['left'])/2
    distance = abs(x1-x2)
    return distance


def from_cor_dict(v,inc=0):
    
    img_width = v['width']
    
    temp_dic={}
    temp_dic['word']=v['word']
    temp_dic['width']= v['area']['width'] * (670 / img_width)
    temp_dic['height']= v['area']['height'] * (670 / img_width)
    temp_dic['pg_no']= v['area']['page']
    temp_dic['bottom']= v['area']['y'] + v['area']['height']+inc
    temp_dic['top']= v['area']['y']-inc
    temp_dic['right']= v['area']['x'] + v['area']['width']+inc
    temp_dic['left']= v['area']['x']-inc
    
    return temp_dic


def get_value_dict(v,ocr_data,inc):
    
    if inc>100:
        return {}

    re_val_=from_cor_dict(v,inc)
    re_word=re_val_.get('word','')
    re_val={}
    for word in ocr_data:
        if word['top']>=re_val_['top'] and word['bottom']<=re_val_['bottom']:
            if word['left']>=re_val_['left'] and word['right']<=re_val_['right']:
                if re_word==word['word']:
                    re_val=word
                    break
                
    if not re_val:
        re_val=get_value_dict(v,ocr_data,inc+10)

    return re_val



def get_cont(row_top,sorted_words,width,col_f,api=''):
    cont={}
    print(F"cont is {cont} and width is {width} and row_top is {row_top} and {col_f}")
    if width<0:
        return cont
    
    for word in sorted_words:
        if not api:
            if word['top']<row_top:
                # print(F"here is {word} and {col_f}")
                if int(abs(word['right']-word['left']))>width:
                    word['hypth']=calculate_distance(word, col_f)
                    word['context_width']=width
                    word['position']='above'
                    cont=word
                    break
        else:
            if word['top']>row_top:
                # print(F"here is {word} and {col_f}")
                if int(abs(word['right']-word['left']))>width:
                    word['hypth']=calculate_distance(word, col_f)
                    word['context_width']=width
                    word['position']='below'
                    cont=word
                    break
    if not cont:
        return get_cont(row_top,sorted_words,width-100,col_f,api)
    else:
        return cont


def is_alphanumeric(word):
    return any(char.isalpha() for char in word) and any(char.isdigit() for char in word)


def line_wise_ocr_data(words):
    ocr_word=[]
    sorted_words = sorted(words, key=lambda x: x["top"])

    # Group words on the same horizontal line
    line_groups = []
    current_line = []
    for word in sorted_words:
        if not current_line:
            current_line.append(word)
        else:
            diff=abs(word["top"] - current_line[-1]["top"])
            if diff < 5:
                # Word is on the same line as the previous word
                current_line.append(word)
            else:
                # Word is on a new line
                line_groups.append(current_line)
                current_line = [word]

        
    if current_line:
        line_groups.append(current_line)

                
    return line_groups   

def get_col(sorted_words,re_val):

    if len(sorted_words)==0:
        return '',{},0
    
    next_values=[]
    k_left=re_val["left"]
    min_dif=10000
    
    
    for line in sorted_words:
        
        if line:
            diff_ver=abs(abs((line[0]["top"]+line[0]["bottom"])/2) - abs((re_val["top"]+re_val["bottom"])/2))
           
            if diff_ver<min_dif:
                print(f"diff_ver is {diff_ver} for line {line}")
                next_values=[]
                found_line=[]
                min_dif=diff_ver
                for word in line:
                    found_line.append(word)
                    if k_left>word['left']:
                        next_values.append(word)
    new_sorted_words=[]
    for line in sorted_words:
        tem=[]
        for word in line:
            if word not in found_line:
                tem.append(word)
        new_sorted_words.append(tem)

    print(F"next_values for value for col_head are is {next_values,  min_dif} ")
    
    sorted_list = sorted(next_values, key=lambda x: x['right'])
    col_head=''
    
    for item in sorted_list:
        t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
        t_num=re.sub(r'[^a-zA-Z]', '', item['word'])
        if (not t or len(t)<5) and len(t_num) < 4:
            continue
        if t.isalpha():
            col_head = item['word']
            col_f=item
            break       
    if not col_head:
        print(F"sorted_list is {sorted_list}")
        for item in sorted_list:
            t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
            t_num=re.sub(r'[^a-zA-Z]', '', item['word'])
            if not t or len(t)<5 or len(t_num)<5:
                continue
            t_fl=False
            numeric_pattern = re.compile(r'^[\d,]+(\.\d+)?$')
            l_i=item['word'].split()
            for i in l_i:
                if numeric_pattern.match(i):
                    t_fl=True
                    break
            if not t_fl or len(t)>10:
                col_head = item['word']
                col_f=item
                break

    if not col_head:
        return get_col(new_sorted_words,re_val)
    else:
        return col_head,col_f,min_dif


def get_top_header(sorted_list_head,col_f,flag=False):

    row_head_list=[]

    for item in sorted_list_head:
        print(F" item is {item}")
        i_cen=abs(item['right']+item['left']/2)
        t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
        t_alp=re.sub(r'[^a-zA-Z]', '', item['word'])
        if not t:
            continue
        numeric_headers=['90','0','180','91','120','121','150','151','181','270','271','365']
        if not row_head_list:
            row_top=item['top']
        if t and len(t)>=4 and t.isalpha():
            print(F" item is {item}")
            if abs(item['bottom']-row_top)<=(2*abs(item['top']-item['bottom'])):
                print(abs(item['bottom']-row_top),abs(item['top']-item['bottom']))
                if col_f['right']<item['left']:
                    row_head_list.append(item['word'])
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
                        row_head_list.append(item['word'])
                        row_top=item['top']
        print(F"row_head_l is {row_head_list}")

    return row_head_list,row_top



def get_headers(act_val,re_val_,case_id,tenant_id):

    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
    document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
    ocr_data_all=json.loads(document_id_df_all)
    ocr_data=ocr_data_all[int(re_val_['pg_no'])]

    re_li=act_val.get('ocr_data',[])
    if len(re_li) == 1:
        re_val=re_li[0]
    elif len(re_li) == 0:
        return '','',{}
    else:
        re_val=get_value_dict(act_val,ocr_data,0)
    if not re_val:
        re_val=re_val_
    print(F"re_val got is {re_val}")
    next_values=[]
    return_list=[]
    
    k_left=re_val["left"]
    centorid=(re_val['left']+re_val['right'])/2
    sorted_words=line_wise_ocr_data(ocr_data)

    for line in sorted_words:
            
            flag=False
            for word in line:
                #nned to do based in center point top -bottom
                if (word['left']<=centorid<=word['right'] or word['left']<=re_val['right']<=word['right'] or word['left']<=re_val['left']<=word['right'] )and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['right']<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-10<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-20<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-30<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-40<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
            if flag == False:
                sorted_left = sorted(line, key=lambda x: x["left"],reverse=True)
                for wo in sorted_left:
                    if wo['right']<re_val['left'] and abs(wo['right']-re_val['left'])<100 and wo['top'] < re_val['top']:
                        if return_list and return_list[-1]['bottom']<wo['top']:
                            return_list.append(wo)
                            break
                        else:
                            return_list.append(wo)
                            break
            
    col_head=''
    row_head_list=[]
    row_head=''
    cont={}
    

    col_head,col_f,diff_col=get_col(sorted_words,re_val)

    print(F"return_list for value for row_head are is {return_list} ")

    #sort in reverse order
    sorted_list_head = sorted(return_list, key=lambda x: x['top'] ,reverse=True)

    row_head_list,row_top=get_top_header(sorted_list_head,col_f)

    if not row_head_list:
        row_head_list,row_top=get_top_header(sorted_list_head,col_f,True)

    print(F"final row_head_list  are {row_head_list} and {col_head} ")
    for head in row_head_list:
        row_head=row_head+' '+head

    print(F"final row_head and col_head are {row_head} and {col_head} ")
    sorted_words = sorted(ocr_data, key=lambda x: x["top"], reverse=True)
    cont=get_cont(row_top,sorted_words,1000,col_f)
    print(cont)
    if not cont:
        cont=get_cont(row_top,sorted_words,1000,col_f,True)
    print(F"final cont is {cont} ")
    
    print(F"row_head col_head is {row_head, col_head  } ")
    return col_head,row_head,cont,diff_col


def get_training_data(case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    temaplate_db=DB('template_db',**db_config)
    extraction=DB('extraction',**db_config)
    query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
    CUSTOMER_NAME = extraction.execute_(query)['customer_name'].to_list()[0]
    logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
    PRIORITY_DATA={}
    PRIORITY_CONTEXT={}
    PRIORITY_FORMULA={}
    if CUSTOMER_NAME:
        CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
        query = f"SELECT `PRIORITY_DATA`,`PRIORITY_CONTEXT`,`PRIORITY_FORMULA` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
        data = temaplate_db.execute_(query)
        logging.info(f"CUSTOMER_NAME got is {data}")
        try:
            PRIORITY_DATA=data['PRIORITY_DATA'].to_list()
            if PRIORITY_DATA:
                PRIORITY_DATA=json.loads(PRIORITY_DATA[0])
            else:
                PRIORITY_DATA={}
            PRIORITY_CONTEXT=data['PRIORITY_CONTEXT'].to_list()
            if PRIORITY_CONTEXT:
                PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT[0])
            else:
                PRIORITY_CONTEXT={}
            PRIORITY_FORMULA=data['PRIORITY_FORMULA'].to_list()
            if PRIORITY_FORMULA:
                try:
                    PRIORITY_FORMULA=json.loads(PRIORITY_FORMULA[0])
                    if not PRIORITY_FORMULA[0] or PRIORITY_FORMULA[0] == None:
                        PRIORITY_FORMULA={}
                except:
                    PRIORITY_FORMULA={}
            else:
                PRIORITY_FORMULA={}
        except Exception as e:
            logging.exception(f"exception occured ..{e}")
            pass

    return PRIORITY_CONTEXT,PRIORITY_DATA,PRIORITY_FORMULA


def update_tarining_data(PRIORITY_DATA,PRIORITY_CONTEXT,PRIORITY_FORMULA,case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    temaplate_db=DB('template_db',**db_config)
    extraction_db=DB('extraction',**db_config)
    query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
    CUSTOMER_NAME = extraction_db.execute_(query)['customer_name'].to_list()[0]
    logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
    if CUSTOMER_NAME:
        CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
        insert_data = {
            'PRIORITY_DATA':json.dumps(PRIORITY_DATA),
            'PRIORITY_CONTEXT':json.dumps(PRIORITY_CONTEXT),
            'PRIORITY_FORMULA':json.dumps(PRIORITY_FORMULA)
        }
        temaplate_db.update('trained_info', update=insert_data, where={'CUSTOMER_NAME': CUSTOMER_NAME})
    return True


def generate_formula(input_formula,mapping):
    parts = []
    current_part = ''
    for char in input_formula:
        if char in ['+', '-', '*', '/']:
            parts.append(current_part)
            parts.append(char)
            current_part = ''
        else:
            current_part += char
    parts.append(current_part)
    output_parts = [mapping.get(part, part) for part in parts]
    output_formula = ''.join(output_parts)
    return output_formula


def save_recommended(recommended_changes,cropped_data,formula_data,case_id,extraction_db,tenant_id):

    PRIORITY_CONTEXT,PRIORITY_DATA,PRIORITY_FORMULA=get_training_data(case_id,tenant_id)
    try:
        try:
            PRIORITY_CONTEXT=json.loads(json.loads(PRIORITY_CONTEXT))
        except:
            PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT)
    except:
        PRIORITY_CONTEXT=PRIORITY_CONTEXT
    try:
        PRIORITY_DATA=json.loads(PRIORITY_DATA)
    except:
        PRIORITY_DATA=PRIORITY_DATA
    try:
        PRIORITY_FORMULA=json.loads(PRIORITY_FORMULA)
    except:
        PRIORITY_FORMULA=PRIORITY_FORMULA

    print(F"before changes PRIORITY_DATA is {PRIORITY_DATA} ")
    print(F"before changes PRIORITY_FORMULA is {PRIORITY_FORMULA} ")
    print(F"before changes PRIORITY_CONTEXT is {PRIORITY_CONTEXT} ")
    final_dict={}
    for col,rec in recommended_changes.items():
        query = f"select `{col}` from `ocr` where case_id ='{case_id}'"
        ac = extraction_db.execute_(query)[col].to_list()[0]
        try:
            ac=json.loads(ac)
        except:
            ac=ac
        for_temp={}
        print(rec)
        for main_label,lab_changes in rec.items():
            for label,changed_val in lab_changes.items():
                if 'Typed' in label:
                    if col in formula_data and main_label in formula_data[col]:
                        ac[main_label]=formula_data[col][main_label]['final_value']
                    else:
                        ac[main_label]=changed_val
                elif 'Cropped' in label:
                    if main_label in cropped_data:
                        try:
                            print(F"changes is {changed_val}")
                            crop_cor=from_cor_dict(cropped_data[main_label])
                            col_head,row_head,context,diff_val_col=get_headers(cropped_data[main_label],crop_cor,case_id,tenant_id)
                            if col_head:
                                if main_label not in PRIORITY_DATA:
                                    PRIORITY_DATA[main_label]={}
                                PRIORITY_DATA[main_label]={col_head+"@"+str(diff_val_col)+"_crop":row_head}
                                try:
                                    PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT)
                                except:
                                    PRIORITY_CONTEXT=PRIORITY_CONTEXT
                                if main_label not in PRIORITY_CONTEXT:
                                    PRIORITY_CONTEXT[main_label]={}
                                PRIORITY_CONTEXT[main_label][col_head+"@"+str(diff_val_col)+row_head]=context
                            if  col in formula_data and main_label in formula_data[col]:
                                ac[main_label]=formula_data[col][main_label]['final_value']
                            else:
                                ac[main_label]=changed_val
                            for_temp[main_label]=col_head+"_"+row_head
                        except Exception as e:
                            print(F"################# there is a issue here {e}")
                            ac[main_label]=changed_val
                else:
                    if main_label in PRIORITY_DATA:
                        if '_crop' not in PRIORITY_DATA[main_label]:
                            tem=label.split('_')
                            if len(tem)==2:
                                PRIORITY_DATA[main_label]={tem[0]:tem[1]}
                    if main_label in ac and 'a.v' in ac[main_label]:
                        if  col in formula_data and main_label in formula_data[col]:
                                ac[main_label]=formula_data[col][main_label]['final_value']
                        else:
                            try:
                                tem_a=ac[main_label]['a.v']
                                ac[main_label]['a.v']={label:changed_val}
                                rem=[]
                                for key in ac[main_label]['r.v']:
                                    if key in changed_val:
                                        rem.append(key)
                                for ke in rem:
                                    ac[main_label]['r.v'].pop(ke) 
                                if 'r.v' in ac[main_label]:
                                    ac[main_label]['r.v'].update(tem_a) 
                            except:
                                ac[main_label]=changed_val
                    else:
                        ac[main_label]=changed_val
            print(F"after changes col is {col} ")
            if col not in PRIORITY_FORMULA:
                PRIORITY_FORMULA[col]=[] 
            if col in formula_data:
                output_formula =generate_formula(formula_data[col][main_label]['formula'],for_temp)
                print(F" ##### update formula is {output_formula}")
                PRIORITY_FORMULA[col].append(output_formula)
            final_dict[col]=ac

    print(F"after changes PRIORITY_DATA is {PRIORITY_DATA} ") 
    if PRIORITY_DATA:
        update_tarining_data(PRIORITY_DATA,PRIORITY_CONTEXT,PRIORITY_FORMULA,case_id,tenant_id)

    for col,value in final_dict.items():
        chunk_size = 4000 
        value=json.dumps(value)
        logging.info(f"Updated JSON data: {value}")

        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {col} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            sql += "TO_CLOB('" + chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
        
        extraction_db.execute_(sql)
        
    return True

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


def from_high_cor(words):
    try:
        temp = {}
        # temp['word'] = words['word']
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


def update_tarining_date(date,unsubcribed_fields,user_tarined_data,case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    temaplate_db=DB('template_db',**db_config)
    extraction_db=DB('extraction',**db_config)

    query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
    CUSTOMER_NAME = extraction_db.execute_(query)['customer_name'].to_list()[0]
    logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
    if CUSTOMER_NAME:
        CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()

    queue_db = DB('queues', **db_config)
    query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
    document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
    ocr_data_all=json.loads(document_id_df_all)

    word=''
    word_dict={}
    if date:
        top_values = [item['top'] for item in date]
        bottom_values = [item['bottom'] for item in date]

        min_top = min(top_values)
        min_top=(min_top-2)
        max_bottom = max(bottom_values)
        max_bottom=(max_bottom+2)

        ocr_data=ocr_data_all[int(date[0]['pg_no'])-1]

        words=[]
        for word in ocr_data:
            if word['top']>=min_top and word['bottom']<=max_bottom:
                words.append(word['word'])

        word=" ".join(item for item in words)

        word_dict={"word":word,"bottom":max_bottom,"top":min_top}

    trained_data={}
    saved_data={}
    if user_tarined_data:

        USER_TRAINED_HIGHLIGHTS={}

        try:
            query = f"SELECT `USER_TRAINED_FIELDS` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
            saved_data = temaplate_db.execute_(query)['USER_TRAINED_FIELDS'].to_list()
            if saved_data:
                saved_data=json.loads(saved_data[0])
            else:
                saved_data={}

            print(F"saved_data is {saved_data}")
            for key,value in user_tarined_data.items():

                if value=='context_clear':
                    saved_data.pop(key)
                    continue

                USER_TRAINED_HIGHLIGHTS[key]=[]
             
                if "word" in value:
                    value_box=combine_dicts(value['word'])
                    print(value_box)
                    USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(value_box))
                  
                if "header" in value:
                    header_box=combine_dicts(value['header'])
                    print(header_box)
                    USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(header_box))
                  
                if "context" in value:
                    context_box=combine_dicts(value['context'])
                    print(context_box)
                    USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(context_box))
                  
                print(F"USER_TRAINED_HIGHLIGHTS is {USER_TRAINED_HIGHLIGHTS}")
                
                if "word" in value and "header" in value and 'context' in value:
                    trained_data[key]={}
                    temp={}
                    trained_header=[]
                    trained_context=[]
                    page_no=value['word'][0]['pg_no']-1
                    ocr_data=ocr_data_all[page_no]
                    value_box=combine_dicts(value['word'])
                    header_box=combine_dicts(value['header'])
                    context_box=combine_dicts(value['context'])

                    diff_head=calculate_distance(header_box,context_box)
                    diff_value_head=calculate_value_distance(header_box,value_box)

                    sorted_words=line_wise_ocr_data(ocr_data)
                    col_head,col_f,diff_col=get_col(sorted_words,value_box)

                    max_width=0
                    for word in ocr_data:
                        if word["left"]>=header_box['left']-10 and  word["right"]<=header_box['right']+10:
                            if  word["top"]>=header_box['top']-10 and  word["bottom"]<=header_box['bottom']+10:
                                trained_header.append(word['word'])
                        if word["left"]>=context_box['left'] and  word["right"]<=context_box['right']:
                            width=word["right"]-word["left"]
                            if  word["top"]>=context_box['top'] and  word["bottom"]<=context_box['bottom']:
                                if max_width<width:
                                    word['hypth']=calculate_distance(col_f,word)
                                    word['context_width']=width
                                    max_width=width
                                    if context_box['top']<col_f['top']:
                                        word['position']='above'
                                    else:
                                        word['position']='below'
                                    trained_context=word

                    temp=[diff_col,diff_head,col_head,trained_header,trained_context,diff_value_head,True]
                    trained_data[key]=temp
               

            query=f"UPDATE OCR SET USER_TRAINED_HIGHLIGHTS = '{json.dumps(USER_TRAINED_HIGHLIGHTS)}' WHERE case_id='{case_id}'"
            query2=f"UPDATE OCR SET user_trained_data = '{json.dumps(user_tarined_data)}' WHERE case_id='{case_id}'"
            extraction_db.execute_(query)
            extraction_db.execute_(query2)

        except Exception as e:
            saved_data={}
            print(F" ######## error is {e}")
            pass
            
    if CUSTOMER_NAME:
        saved_data.update(trained_data)
        insert_data={}
        if saved_data:
            insert_data["USER_TRAINED_FIELDS"]=json.dumps(saved_data)
        if word_dict:
            insert_data["TRAINED_DATE"]=json.dumps(word_dict)
        if unsubcribed_fields:
            insert_data["UNSUBCRIBED_FIELDS"]=json.dumps(unsubcribed_fields)
        if insert_data:
            temaplate_db.update('trained_info', update=insert_data, where={'CUSTOMER_NAME': CUSTOMER_NAME})

    return True


def save_changes_m(case_id, data, tenant_id, queue_id, message_data={}):
    attr = ZipkinAttrs(
        trace_id=case_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id if tenant_id is not None else ''
    )

    with zipkin_span(
            service_name='save_changes',
            zipkin_attrs=attr,
            span_name='save_changes',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            logging.info(f'Data recieved for save changes: {data}')

            db_config['tenant_id'] = tenant_id

            fields = data.get('fields', {})
            total_fields=fields.keys()

            column_dict = {}

            if "" in fields:
                fields.pop("")

            try:
                extraction_db = DB('extraction', **db_config)
                edited_fields= data.get('editedFields', [])
                selectedUnitsValue= data.get('selectedUnitsValue', '')
                selectedUnitsValue = json.dumps(selectedUnitsValue)
                query = f"SELECT EDITED_FIELDS FROM ocr WHERE case_id = '{case_id}'"
                query_data = extraction_db.execute_(query)['edited_fields'].to_list()[0]
                if query_data:
                    query_data=json.loads(query_data)
                    query_data.extend(edited_fields)
                    query_data=json.dumps(query_data)
                    query=f"UPDATE OCR SET EDITED_FIELDS = '{query_data}' WHERE case_id='{case_id}'"
                    extraction_db.execute_(query)
                else:
                    edited_fields=json.dumps(edited_fields)
                    query=f"UPDATE OCR SET EDITED_FIELDS = '{edited_fields}' WHERE case_id='{case_id}'"
                    extraction_db.execute_(query)

                
                query=f"UPDATE OCR SET SELECTED_UNITS = '{selectedUnitsValue}' WHERE case_id='{case_id}' "
                extraction_db.execute_(query)
            
            except:
                pass

            field_changes = data.get('field_changes', [])
            cropped_data = data.get('cropped_data', [])
            verify_operator = data.get('user', None)
            
            logging.debug(f"Case id is {case_id} Fields going to save in database are {fields}")
           
            '''
            changes:- standardizing the function to work on multiple table (previouslt restricted on ocr itself)
            author:- Amara Sai Krishna Kumar
            '''
            try:
                logging.info(f"in try block for updating table")
                field_accuracy_(data,tenant_id,case_id)
                # update_table(queue_db, case_id, "", data)
            except Exception as e:
                logging.info(f"in exception {e}")
                pass

            if len(field_changes) != 0:
                changed_fields = {k: fields[k]
                                for k in field_changes if k in fields}
                logging.info(f"fields changed for {case_id} are {changed_fields}")
               
                # manipulate highlight data with the received UI request data (crooped_data)
                list_of_dicto = dict_split_wise_table(changed_fields)
                for i in range(len(list_of_dicto)):
                    for total_table_name,column_dict in list_of_dicto[i].items():
                        data_base = total_table_name.split('.')[0]
                        table = total_table_name.split('.')[1]
                        db_conn = DB(data_base, **db_config)
                        custom_table=['STOCK STATEMENT','DEBITORS STATEMENT','CREDITORS']
                        for column in custom_table:
                            if column in changed_fields:
                                table='custom_table'
                                column_dict=changed_fields
                        logging.info(f'##table {table} #column_dict {column_dict}')        
                        if table !='ocr':
                            db_conn.update(table, update= column_dict, where={'case_id': case_id})
                            
                        else:
                            query = f"select case_id,highlight from {table} where case_id='{case_id}'"
                            case_data = db_conn.execute_(
                                query).to_dict(orient='records')
                            logging.info(f"case_data--{case_data}")
                            if not case_data[0]['highlight']:
                                case_data[0]['highlight']='{}'
                            highlight = ast.literal_eval(case_data[0]['highlight'])
                            
                            try:
                                if len(column_dict) != 0:
                                    if len(cropped_data) != 0:
                                        keys_ = list(highlight.keys())
                                        for k_, v_ in cropped_data.items():
                                            print(v_)
                                            for k,v in v_.items():
                                                # print(k)
                                                img_width = v['width']
                                                new_dict = {'x': v['area']['x'] * (670 / img_width), 'y': v['area']['y'] * (670 / img_width), 'width': v['area']['width'] * (670 / img_width), 'height': v['area']['height'] * (670 / img_width), 'page': v['area']['page'], 'bottom': v['area']['y'] + v['area']
                                                            ['height'], 'top': v['area']['y'], 'right': v['area']['x'] + v['area']['width'], 'left': v['area']['x'], 'status': 'Updated Highlight' if k in keys_ else 'New Highlight'}
                                                # Updating data to highlight
                                                highlight[k] = new_dict
                                        data.pop('cropped_data', '')
                                        column_dict['highlight'] = json.dumps(highlight)
                                        logging.info(f"column dict iss--{column_dict}")
                                    db_conn.update(table, update=column_dict, where={'case_id': case_id})       
                            except:
                                if len(column_dict) != 0:
                                
                                    if len(cropped_data) != 0:
                                        keys_ = list(highlight.keys())
                                        for k, v in cropped_data.items():
                                            img_width = v['width']
                                            new_dict = {'x': v['area']['x'] * (670 / img_width), 'y': v['area']['y'] * (670 / img_width), 'width': v['area']['width'] * (670 / img_width), 'height': v['area']['height'] * (670 / img_width), 'page': v['area']['page'], 'bottom': v['area']['y'] + v['area']
                                                        ['height'], 'top': v['area']['y'], 'right': v['area']['x'] + v['area']['width'], 'left': v['area']['x'], 'status': 'Updated Highlight' if k in keys_ else 'New Highlight'}
                                            # Updating data to highlight
                                            highlight[k] = new_dict
                                        data.pop('cropped_data', '')
                                        column_dict['highlight'] = json.dumps(highlight)
                                        logging.info(f"column dict in expection iss--{column_dict}")
                                    db_conn.update(table, update=column_dict, where={'case_id': case_id})
            recommended_changes = data.get('recommended_changes', {})
            formula_data = data.get('formula_data', {})
            if recommended_changes:
                save_recommended(recommended_changes,cropped_data,formula_data,case_id,extraction_db,tenant_id)
            date_cropped = data.get('date_cropped', {})
            unsubcribed_fields = data.get('unsubscribed_fields', [])
            user_tarined_data = data.get('user_trained_data', [])
            if date_cropped or unsubcribed_fields or user_tarined_data:
                update_tarining_date(date_cropped,unsubcribed_fields,user_tarined_data,case_id,tenant_id)
              
            return {'flag':True,'message': 'Saved changes.','data':column_dict}
        except:
            logging.exception('Something went wrong saving changes. Check trace.')
            return {'flag':False,'message': 'Something went wrong saving changes. Check logs.','data':{}}


@app.route('/save_changes', methods=['POST', 'GET'])
def save_changes_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    all_data = request.get_json(force=True)
    #Parsing the data from execute function(UI) request data
    try:

        data = json.loads(all_data['ui_data'])
    except:
        data = all_data['ui_data']
    logging.info(f'Message UI Data: {data}')

    user = data.get('user', None)
    session_id = data.get('session_id', None)
    variables_check = True

    case_id = data.get('case_id', None)
    if case_id is None:
        case_id=all_data.get("case_id",None)
    # functions = data['functions']
    tenant_id = data.get("tenant_id",None)
    if tenant_id is None:
        tenant_id=all_data.get("tenant_id",None)

    queue_id = data.get('queue_id',None)
    if queue_id is None:
        queue_id=all_data.get("queue_id",None)

    if case_id is None or tenant_id is None or queue_id is None:
        logging.warning(f'Either Case id or tenant id is None please check. UI data: [{data}]')
        return jsonify({'flag': False, 'message': 'Either Case id or tenant id is None please check'})

    db_config['tenant_id'] = tenant_id

    # Call the function
    try:
        logging.debug('Calling function `save_changes`')
        result = save_changes_m(case_id, data, tenant_id, queue_id)

        if not result['flag']:
            return jsonify({'flag': False, 'message': 'Error in Save Changes'})
        logging.info(f'response of save changes function {result}')
        return_data = {'flag': True, 'data': {'message':'Saving Data Sucess'}}
    except Exception as e:
        result = {'data':{}}
        return_data =  {'flag': False, 'message': 'Error in Save Changes'}
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

    audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id, 
                        "api_service": "save_changes", "service_container": "save_changes", "changed_data": json.dumps(result['data']),
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(return_data), "trace_id": case_id, "session_id": session_id,"status":str(return_data['flag'])}
    insert_into_audit(case_id, audit_data)

    return jsonify(return_data)
