import math
import os
import json
import re
import regex
import cv2
import numpy as np
from os import path
import ntpath
import time
import ast

from pathlib import Path
from collections import defaultdict
from db_utils import DB
from ace_logger import Logging
# from flask import Flask, request, jsonify
# from flask_cors import CORS

# from app import app
# app = Flask(__name__)
# cors = CORS(app)

logging = Logging()

db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
}

try:
    with open('app/parameters.json') as f:
        parameters = json.loads(f.read())
except:
    with open('parameters.json') as f:
        parameters = json.loads(f.read())

BIG_FONT_X_PAD = parameters['BIG_FONT_X_PAD']
BIG_FONT_Y_PAD = parameters['BIG_FONT_Y_PAD']
SMALL_FONT_X_PAD = parameters['SMALL_FONT_X_PAD']
SMALL_FONT_Y_PAD = parameters['SMALL_FONT_Y_PAD']
LINES_SCALE = parameters['LINES_SCALE']
OVERLAP_MARGIN_HORI_PERC = parameters['OVERLAP_MARGIN_HORI_PERC']
OVERLAP_MARGIN_VER_PERC = parameters['OVERLAP_MARGIN_VER_PERC']


def get_frame_info(cur_frame):
    return str(cur_frame[0][1]) + ' @ ' + str(cur_frame[0][2]) + ' : ' + cur_frame[0][3] + '()'

def get_cords_words(ocr_data):
    coords,words=[],[]
    for page in ocr_data:
        for cord in page:
            words.append(cord['word'])
            coords.append([cord['left'],cord['top'],cord['right'],cord['bottom']])
    return coords, words

def get_cords_words_nikhil(ocr_data,page_number):
    coords,words=[],[]
    page=ocr_data[page_number]
    for cord in page:
        words.append(cord['word'])
        coords.append([cord['left'],cord['top'],cord['right'],cord['bottom']])
    return coords, words

def get_ocr_data(case_id, tenant_id):
    db_config['tenant_id']=tenant_id
    queue_db = DB('queues', **db_config)

    query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
    params = [case_id]
    ocr_info = queue_db.execute_(query, params=params)

    if ocr_info.empty:
       ocr_data = {}
       logging.warning('OCR data is not in DB or is empty.')
    else:
       try:
          ocr_data = json.loads(json.loads(list(ocr_info.ocr_data)[0]))
       except:
          ocr_data = json.loads(list(ocr_info.ocr_data)[0])
    return ocr_data

def get_ocr_data_parsed(tenant_id,case_id,document_id):
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)

    query = f"SELECT * FROM `ocr_info` WHERE `case_id`='{case_id}' and `document_id`='{document_id}'"
    
    ocr_info = queue_db.execute_(query)
    
    if ocr_info is None or ocr_info is False:
        ocr_data = {}
        print('OCR data is not in DB or is empty.')
    else:
        try:
            ocr_data = json.loads(json.loads(list(ocr_info.ocr_parsed)[0]))
            
        except:
            ocr_data = json.loads(list(ocr_info.ocr_parsed)[0])
            
    return ocr_data

def get_ocr_data_parsed_cet(tenant_id,case_id,document_id):
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)

    query = f"SELECT * FROM `ocr_info` WHERE `case_id`='{case_id}' and `document_id`='{document_id}'"
    
    ocr_info = queue_db.execute_(query)
    
    if ocr_info is None or ocr_info is False:
        ocr_data = {}
        print('OCR data is not in DB or is empty.')
    else:
        try:
            ocr_data = json.loads(json.loads(list(ocr_info.ocr_word)[0]))
            
        except:
            ocr_data = json.loads(list(ocr_info.ocr_word)[0])
            
    return ocr_data

def get_cords_words_parsed(ocr_data, page_number):
    coords, words = [], []
    
    page_key = str(page_number+1)
    
    if page_key in ocr_data:
        page_data = ocr_data[page_key]
        
        for sent_no, sent in page_data.items():
            words.append(sent['text'])
            coords.append([sent['left'], sent['top'], sent['right'], sent['bottom']])
    else:
        print(f"Page number {page_number} data is not available.")
    
    return coords, words

def get_file_path(case_id,document_id, source_folder, tenant_id):
    """
    Author : Akshat Goyal

    :param case_id:
    :param tenant_id:
    :return:
    """
    try:
        db = DB('io_configuration', tenant_id=tenant_id, **db_config)
        input_config = db.get_all('input_configuration')
        output_config = db.get_all('output_configuration')

        if (input_config.loc[input_config['type'] == 'Document'].empty
                or output_config.loc[input_config['type'] == 'Document'].empty):
            message = 'Input/Output not configured in DB.'
            logging.error(message)
            return ''
        else:
            input_path = input_config.iloc[0]['access_1']
            output_path = output_config.iloc[0]['access_1']

        queue_db = DB('queues', tenant_id=tenant_id, **db_config)
        query = f"select id, file_name from process_queue where document_id = '{document_id}'"
        file_name = list(queue_db.execute(query)['file_name'])[0]

        try:
            file_name = output_path + '/' + case_id + '/' + file_name
        except:
            file_name = output_path + '/' + case_id + '.pdf'

        file_path = Path(source_folder) / file_name
    except:
        file_path = ''

    return file_path


def get_area_intersection(box, word, area_of_word):
    box_l, box_r, box_b, box_t = box
    word_l, word_r, word_b, word_t = word

    mid_x = word_l + (word_r - word_l) / 2
    mid_y = word_t + (word_b - word_t) / 2

    width = word_r - word_l
    height = word_b - word_t

    margin_wid = (width * 10) / 100
    margin_hig = (height * 10) / 100

    x_condition = False
    y_condition = False

    if box_l < mid_x < box_r:
        x_condition = True

    if box_t < mid_y < box_b:
        y_condition = True

    return x_condition and y_condition


def percentage_inside(box, word):
    '''
    Get how much part of the word is inside the box
    '''
    box_l, box_r, box_b, box_t = box
    word_l, word_r, word_b, word_t = word

    area_of_word = (word_r - word_l) * (word_b - word_t)
    area_of_box = (box_r - box_l) * (box_b - box_t)

    area_of_intersection = get_area_intersection(box, word, area_of_word)

    if area_of_intersection:
        return 1
    else:
        return 0


def percentage_inside_lazy(box, word):
    if not box or not word:
        return 0
    box = box['left'], box['right'], box['bottom'], box['top']
    word = word['left'], word['right'], word['bottom'], word['top']

    return percentage_inside(box, word)


def calulcate_centroid(coordinates):
    """
    Author : Akshat Goyal

    Args ;
        coordinates :
            {
                'left' : int,
                'right' : int,
                'top' : int,
                'bottom' : int
            }

    Return :
        {
            'x':mid_x,
            'y':mid_y
        }
    """
    mid_x = coordinates['left'] + (coordinates['right'] - coordinates['left']) / 2
    mid_y = coordinates['top'] + (coordinates['bottom'] - coordinates['top']) / 2

    return {'x': mid_x, 'y': mid_y}


def centroid_hunt(word_list):
    """
    Author : Akshat Goyal

    Args:
        word_list :
            [
                {
                    'word' : str,
                    coordinates
                }
            ]

    Return:
        [
            {
                ''
                'mid_x':int
                'mid_y':int
            }
        ]

    """
    for word in word_list:
        temp = calulcate_centroid(word)
        word['mid_x'] = int(temp['x'])
        word['mid_y'] = int(temp['y'])

    return word_list


def caculate_dis(box1, box2):
    return calculate_point_distance_from_other_points([box1], [box2])[0]


def calculate_point_distance_from_other_points(remaining_coords, cluster_coord):
    """
    Author : Akshat Goyal
    """
    all_dist = []
    mid_x = 0
    mid_y = 0
    points_in_cluster = len(cluster_coord)

    for point in cluster_coord:
        temp_mid_x = point['left'] + (point['right'] - point['left']) / 2
        temp_mid_y = point['top'] + (point['bottom'] - point['top']) / 2

        mid_x += temp_mid_x
        mid_y += temp_mid_y

    if points_in_cluster:
        mid_x /= points_in_cluster
        mid_y /= points_in_cluster

    normalized_coords = []

    for point in remaining_coords:
        temp_mid_x = (point['left'] + (point['right'] - point['left']) / 2) - mid_x
        temp_mid_y = (point['top'] + (point['bottom'] - point['top']) / 2) - mid_y

        normalized_coords.append([temp_mid_x, temp_mid_y])

    for remaining_coord in normalized_coords:
        tot_dis = 0
        tot_dis = math.hypot(remaining_coord[0], remaining_coord[1])

        all_dist.append(tot_dis)

    return all_dist


def get_field_data(tenant_id):
    db = DB('queues', tenant_id=tenant_id, **db_config)
    try:
        tab_df = db.get_all('tab_definition')
        ocr_tab_id = tab_df.loc[tab_df['source'] == 'ocr'].index.values.tolist()

        tab_list = ', '.join([str(i) for i in ocr_tab_id])
        logging.debug(f'Tab List: {tab_list}')
        query = f'SELECT * FROM `field_definition` WHERE `tab_id` in ({tab_list})'

        ocr_fields_df = db.execute(query)
        mandatory_fields = list(ocr_fields_df.loc[ocr_fields_df['mandatory'] == 1]['unique_name'])
        logging.debug(f'OCR Fields DF: {ocr_fields_df}')
        fields = list(ocr_fields_df['unique_name'])
        static_field = list(ocr_fields_df.loc[ocr_fields_df['static_field'] == 1]['unique_name'])
    except Exception as e:
        logging.exception(f'Error getting mandatory fields: {e}')
        mandatory_fields = []
        fields = []
        static_field = []
        is_keywords_okay_set = []

    return mandatory_fields, fields, static_field


def get_ocr_db(case_id, tenant_id):
    """
    """
    queue_db = DB('queues', tenant_id=tenant_id, **db_config)

    query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
    params = [case_id]
    ocr_info = queue_db.execute(query, params=params)
    try:
        ocr_data_list = json.loads(json.loads(list(ocr_info.ocr_data)[0]))
    except:
        ocr_data_list = json.loads(list(ocr_info.ocr_data)[0])

    return ocr_data_list


def ocrDataLocal(T, L, R, B, ocrData):
    '''
        Parameters : Boundaries of Scope
        Output     : Returns that part of ocrdata which is confined within given boundaries
    '''
    ocrDataLocal = []
    for data in ocrData:
        if percentage_inside([L, R, B, T], [data['left'], data['right'], data['bottom'], data['top']]) > parameters[
            'overlap_threshold']:
            # if (data['left'] + int(0.25 * width) >= L
            #         and data['right'] - int(0.25 * width) <= R
            #         and data['top'] + int(0.5 * height) >= T
            #         and data['bottom'] - int(0.5 * height) <= B):
            ocrDataLocal.append(data)
    return ocrDataLocal


def resize_coordinates(box, resize_factor):
    box["width"] = int(box["width"] / resize_factor)
    box["height"] = int(box["height"] / resize_factor)
    box["y"] = int(box["y"] / resize_factor)
    box["x"] = int(box["x"] / resize_factor)

    return box


def merge_coord(prev, nex):
    if prev['top'] > nex['top']:
        prev['top'] = nex['top']

    if prev['left'] > nex['left']:
        prev['left'] = nex['left']

    if prev['right'] < nex['right']:
        prev['right'] = nex['right']

    if prev['bottom'] < nex['bottom']:
        prev['bottom'] = nex['bottom']

    return prev


def make_scope(values):
    """
    """
    try:
        if values:
            scope = dict(values[-1])
        else:
            scope = {}
    except:
        scope = {}

    for value in values[:-1]:
        scope = merge_coord(scope, value)

    return scope


def get_rel_info(box1, box2, direction=None):
    """
    box1 : key
    box2 : value
    """
    if 'left' not in box1 or 'left' not in box2:
        return {}

    mid1 = (
        box1['left'] + int(abs(box1['left'] - box1['right']) / 2),
        box1['top'] + int(abs(box1['top'] - box1['bottom']) / 2))
    mid2 = (
        box2['left'] + int(abs(box2['left'] - box2['right']) / 2),
        box2['top'] + int(abs(box2['top'] - box2['bottom']) / 2))

    dist = math.hypot(mid2[0] - mid1[0], mid2[1] - mid1[1])
    try:
        angle = math.atan((mid2[1] - mid1[1]) / (mid2[0] - mid1[0]))
        angle = math.degrees(angle)
    except:
        # value at top
        if mid2[1] < mid1[1]:
            angle = -90
        else:
            angle = 90

    if direction:
        if int(angle) in range(-30, 30):
            return 'left'
        elif int(angle) in range(150, 180) or int(angle) in range(-180, -150):
            return 'right'
        elif int(angle) in range(-120, -60):
            return 'bottom'
        else:
            return 'top'

    logging.debug({'dist': dist, 'angle': angle})
    return {'dist': dist, 'angle': angle}


def convert_coord_from_ui(value, page_no=0):
    value['left'] = value.get('x', 0)
    value['top'] = value.get('y', 0)
    value['right'] = value.get('width', 0) + value.get('x', 0)
    value['bottom'] = value.get('height', 0) + value.get('y', 0)

    return value


def convert_coord_for_ui(value, page_no=0):
    ui_coord = {}

    ui_coord['x'] = value.get('left', 0)
    ui_coord['y'] = value.get('top', 0)
    ui_coord['width'] = value.get('right', 0) - value.get('left', 0)
    ui_coord['height'] = value.get('bottom', 0) - value.get('top', 0)
    ui_coord['page'] = page_no
    ui_coord['word'] = value.get('word', '')
    ui_coord['left'] = value.get('left', 0)
    ui_coord['top'] = value.get('top', 0)
    ui_coord['right'] = value.get('right', 0)
    ui_coord['bottom'] = value.get('bottom', 0)
    ui_coord['confidence'] = value.get('confidence', 100)

    return ui_coord


def create_unique_fields(meta_dict, new_trained_info, tenant_id, orphan_fields):
    queue_db = DB('queues', tenant_id=tenant_id, **db_config)

    query = 'select `id`, `field`, `type` from `template_detection_config`'
    unique_field_config = queue_db.execute(query).to_dict('records')

    unique_fields = []

    for unique_field in unique_field_config:
        unique = {}
        if unique_field['type'] == 'ocr':
            unique['type'] = 'ocr'
            unique['field'] = unique_field['field']
            unique['text'] = new_trained_info.get(unique['field'], {}).get('scope_value', {}).get('word', '')

        elif unique_field['type'] == 'meta':
            unique['type'] = 'meta'
            unique['field'] = unique_field['field']
            unique['text'] = meta_dict[unique['field']]
        unique_fields.append(unique)

    if orphan_fields:
        unique = {}
        unique['type'] = 'orphan'
        unique['text'] = orphan_fields
        unique['field'] = ''
        unique_fields.append(unique)

    return unique_fields


def create_new_template(new_trained_info, meta_dict, template_name, tenant_id, orphan_fields=''):
    """
    Author: Akshat Goyal

    :param new_trained_info:
    :param meta_dict:
    :param template_name:
    :param case_id:
    :return:
    """

    trained_db = DB('template_db', tenant_id=tenant_id, **db_config)
    unique_fields = create_unique_fields(meta_dict, new_trained_info, tenant_id, orphan_fields)

    header_ocr = ''
    checkboxes_all = ''
    footer_ocr = ''
    address_ocr = []
    condition = 'or'
    trained_data_column_values = {
        'template_name': template_name,
        'field_data': json.dumps(new_trained_info),
        'header_ocr': header_ocr,
        'footer_ocr': footer_ocr,
        'address_ocr': json.dumps(address_ocr),
        'checkbox_data': json.dumps(checkboxes_all),
        'unique_fields': json.dumps(unique_fields),
        'operator': condition
    }

    trained_db.insert_dict(trained_data_column_values, 'trained_info')

    return True


def update_template(new_trained_info, meta_dict, template_name, tenant_id, orphan_fields=''):
    """
        Author: Akshat Goyal

        :param new_trained_info:
        :param meta_dict:
        :param template_name:
        :param case_id:
        :return:
        """

    trained_db = DB('template_db', tenant_id=tenant_id, **db_config)
    unique_fields = create_unique_fields(meta_dict, new_trained_info, tenant_id, orphan_fields)

    header_ocr = ''
    checkboxes_all = ''
    footer_ocr = ''
    address_ocr = []
    condition = 'or'
    trained_data_column_values = {
        'template_name': template_name,
        'field_data': json.dumps(new_trained_info),
        'header_ocr': header_ocr,
        'footer_ocr': footer_ocr,
        'address_ocr': json.dumps(address_ocr),
        'checkbox_data': json.dumps(checkboxes_all),
        'unique_fields': json.dumps(unique_fields),
        'operator': condition
    }

    trained_db.update(update=trained_data_column_values, table='trained_info', where={'template_name': template_name})

    return True


def get_static_field_set(tenant_id):
    """
    Author : Akshat Goyal

    :param tenant_id:
    :return:
    """
    query = "select `id`, `unique_name` from `field_definition` where `static_field` = 1"
    queue_db = DB('queues', tenant_id=tenant_id, **db_config)

    try:
        static_fields = set(queue_db.execute(query))
    except:
        static_fields = set()

    return static_fields


def table_as_kv(extracted_data_maintable):
    extracted_data_maintable_kv = {}

    table_head = extracted_data_maintable[0]
    nested = False
    if table_head[0][1] == 2:
        nested = True

    table_rows = extracted_data_maintable[1:]
    nested_col = []
    if nested:
        table_rows = extracted_data_maintable[2:]
        nested_col = extracted_data_maintable[1]
    header_names = []
    for head in table_head:
        head[0] = head[0].replace('<b>', '')
        head[0] = head[0].replace('</b>', '')
        if nested:
            if head[1] == 1:
                col_span = head[2]
                head_nest = nested_col[:col_span]
                for i in range(len(head_nest)):
                    # print(head[0]+'.'+head_nest[i][0])
                    header_names.append(head[0] + '.' + head_nest[i][0].strip())
                nested_col = nested_col[col_span:]
            else:
                header_names.append(head[0].strip())
        else:
            header_names.append(head[0].strip())
    # print('header_names',header_names)
    # print(len(header_names))
    for row in table_rows:
        for i in range(len(row)):
            if header_names[i] not in extracted_data_maintable_kv:
                extracted_data_maintable_kv[header_names[i]] = [row[i][0].strip()]
            else:
                extracted_data_maintable_kv[header_names[i]].append(row[i][0].strip())
    return extracted_data_maintable_kv


def prepare_predicted_data(values, field_name, keyword, page_no=0):
    field = {'field': field_name}
    if keyword and 'word' in keyword:
        field['keyword'] = keyword['word']
    else:
        field['keyword'] = ''

    field['value'] = [value['word'] for value in values]
    if field['value']:
        field['value'] = ' '.join(field['value'])
    else:
        field['value'] = ''

    valueCords = {'top': 10000, 'bottom': 0, 'right': 0, 'left': 10000, 'word': ''}
    for value in values:
        valueCords = merge_coord(valueCords, value)

    field['coordinates'] = []
    if values:
        field['coordinates'].append(convert_coord_for_ui(valueCords, page_no))
        if field['keyword']:
            field['coordinates'].append(convert_coord_for_ui(keyword, page_no))
    field['validation'] = ''
    field['keycheck'] = True
    field['page'] = page_no

    return field


def get_is_keywords_okay(tenant_id):
    query = 'select * from field_dict'

    template_db = DB('template_db', tenant_id=tenant_id, **db_config)

    try:
        fields = template_db.execute(query).to_dict(orient='records')
    except:
        fields = []

    d2_field = []
    for field in fields:
        try:
            counter = json.loads(field['method_used_counter'])
        except:
            counter = False
        if not counter:
            continue
        if counter['keyword'] < counter['2d']:
            d2_field.append(field['field_type'])

    return d2_field


def get_last_system_generated_template(tenant_id):
    """
    Author : Akshat Goyal

    :param tenant_id:
    :return:
    """

    template_db = DB('template_db', tenant_id=tenant_id, **db_config)
    query = 'SELECT `id`, `template_name` FROM `trained_info` where template_name like "%system_generated_template%" order by id DESC'

    try:
        template_name = list(template_db.execute(query)['template_name'])[0]
    except:
        template_name = "system_generated_template_0"

    return template_name


def remove_only_value(trained_info):
    """
    Author : Akshat Goyal
    """
    new_dict = {}
    for field, field_data in trained_info.items():
        if "value_only" not in field_data:
            new_dict[field] = field_data

    return new_dict


def merge_ocr_field(list_ocr_word):
    keyCords = {'top': 10000, 'left': 10000, 'right': 0, 'bottom': 0}
    words = []
    to_return = False
    min_confidence = 100
    for word in list_ocr_word:
        keyCords = merge_coord(keyCords, word)
        if min_confidence > word.get('confidence', 100):
            min_confidence = word.get('confidence', 100)

        words.append(word['word'])
        to_return = True
    keyCords['word'] = ' '.join(words)
    keyCords['confidence'] = min_confidence

    if to_return:
        return keyCords
    else:
        return {}


def get_header_keywords(tenant_id):
    """

    :param tenant_id:
    :return:
    """
    table_db = DB('template_db', tenant_id=tenant_id, **db_config)
    query = "select `id`, `field_type`, `table_header_list` from field_dict"
    field_header_keyword = {}
    try:
        header_keywords = table_db.execute(query).to_dict(orient='records')
        for keyword in header_keywords:
            try:
                field_header_keyword[keyword['field_type']] = json.loads(keyword['table_header_list'])
            except:
                pass
    except:
        field_header_keyword = {}

    return field_header_keyword


def get_drop_down_config(tenant_id):
    """
    Akshat Goyal
    """
    static_config = [
        {
            "name": 'context',
            "crops": 1,
            "position": [
                {
                    "left": -50,
                    "top": 0,
                    "width": 50,
                    "height": 20
                }
            ]
        },
        {
            "name": '1 keyword',
            "crops": 1,
            "position": [
                {
                    "left": -50,
                    "top": 0,
                    "width": 50,
                    "height": 20
                }
            ]
        },
        {
            "name": '2D keywords',
            "crops": 2,
            "position": [
                {
                    "left": -50,
                    "top": 0,
                    "width": 50,
                    "height": 20
                },
                {
                    "left": 0,
                    "top": -50,
                    "width": 50,
                    "height": 20
                }
            ]
        }
    ]

    return static_config


def to_image(page, file_path, extension, file_no=None):
    file_name = ''
    if not page:
        return file_name
    file_path = str(file_path)
    file_name = file_path.rsplit(extension, 1)[0] + '_' + str(file_no) + '.jpg'
    parent_dir = path.dirname(file_name)
    logging.debug(f"parent_dir - {parent_dir}")

    try:
        import fitz
        if isinstance(page, fitz.fitz.Page):
            pix = page.getPixmap()
            pix.pillowWrite(file_name, "JPEG")
        else:
            page.save(file_name, 'JPEG')
    except:
        page.save(file_name, 'JPEG')
    # logging.debug(f'saving as{file_name}')
    return file_name


def convert_to_images(file_path, wand_page, page_no):
    try:
        file_path = str(file_path)
        file_name = ntpath.basename(file_path)
        if not file_name.endswith('.jpg'):
            extension = file_name.rsplit('.', 1)[-1]
            file_name_exist = file_path.rsplit(extension, 1)[0] + '_' + str(page_no) + '.jpg'
            if not path.exists(file_name_exist):
                conversion_start = time.time()
                imgpath = to_image(wand_page, file_path, extension, page_no)
                logging.info(f'## DEBUG 7.2.2.1 function name : to_image , input : {wand_page} \n {file_path} \n {extension} \n {page_no} output : {imgpath}')
            else:
                # logging.debug("using old file")
                imgpath = file_name_exist
        else:
            imgpath = file_name
        img = cv2.imread(imgpath)
        w, h = img.shape[:2]
    except Exception as e:
        # logging.exception(f"reading image failed")
        img = np.zeros((1000, 1000, 3), np.uint8)
        imgpath = ''
        w, h = img.shape[:2]
    return img


def open_pdf(file_path, batch=10):
    from pdf2image import convert_from_path
    from PIL import Image, ImageSequence
    images = []
    if not file_path:
        return None
    try:
        file_path = str(file_path)
        file_name = ntpath.basename(file_path).lower()
        if file_name.endswith('tif') or file_name.endswith('tiff'):
            logging.debug('tif conversion')
            im = Image.open(file_path)
            for i, page in enumerate(ImageSequence.Iterator(im)):
                out = page.convert("RGB")
                extension = file_name.rsplit('.', 1)[-1]
                file_name_exist = file_path.rsplit(extension, 1)[0] + '_' + str(i) + '.jpg'

                if not path.exists(file_name_exist):
                    out.save(file_name_exist, "JPEG", quality=100)
                imgpath = file_name_exist
                img = cv2.imread(imgpath)
                images.append(img)
            return images
        elif file_name.endswith('.pdf'):
            i = 0
            try:
                import fitz
                pages = fitz.open(file_path)
            except:
                pages = convert_from_path(file_path, 300, first_page=i, last_page=i + batch, thread_count=1)
                logging.info(f'## DEBUG 7.2.1 function name : convert_from_path , input : file_path={file_path} \n first_page={i} \n last_page={i+batch}  output : {pages}')


            # final_pages.extend(pages)
            for idx, page in enumerate(pages):
                img = convert_to_images(file_path, page, i + idx)
                logging.info(f'## DEBUG 7.2.2 function name : convert_to_images , input : {file_path} \n {page} output : {img}')
                images.append(img)
            i += batch + 1
            logging.info(f"returning images: {images}")
            return images
        elif file_name.endswith('.jpg') or file_name.endswith('.jpeg') or file_name.endswith('.png'):
            img = cv2.imread(file_path)
            images.append(img)
            return images
        else:
            return None
    except Exception as e:
        logging.exception(f'open-pdf{e}')
        return None
        pass


def remove_all_except_al_num(text):
    to_return = re.sub('[,.!@#$%^&*()\_\-=`~\'";:<>/?]', '', text.lower())
    to_return = to_return.replace(' ', '')
    return to_return


def convert_ocrs_to_char_dict_only_al_num(raw_haystack):
    char_index_list = []
    haystack = []

    # real_haystack_list = []
    for hay in raw_haystack:
        for old_straw in hay['word']:
            straw = remove_all_except_al_num(old_straw)
            if straw:
                temp = {}
                temp['char'] = straw.lower()
                temp.update(hay)

                haystack.append(temp['char'])
                char_index_list.append(temp)
                # real_haystack_list.append(temp)

    return char_index_list, ''.join(haystack)


def get_pre_processed_char(ocr_data):
    """
    """
    pre_processed_char = []
    for page in ocr_data:
        page = sort_ocr(page)
        char_index_list, haystack = convert_ocrs_to_char_dict_only_al_num(page)
        pre_processed_char.append([char_index_list, haystack])

    return pre_processed_char


def merge_ocr_field(list_ocr_word):
    keyCords = {'top': 10000, 'left': 10000, 'right': 0, 'bottom': 0}
    words = []
    to_return = False
    for word in list_ocr_word:
        keyCords = merge_coord(keyCords, word)
        words.append(word['word'])
        to_return = True
    keyCords['word'] = ' '.join(words)

    if to_return:
        return keyCords
    else:
        return {}


def hereditary_to_ui_coordinates(heritage, coordinate_list):
    if not heritage:
        return coordinate_list

    keyword = convert_coord_for_ui(heritage['keyword'])
    keyword['child'] = []
    keyword['id'] = len(coordinate_list)
    coordinate_list.append(keyword)

    keyword_index = len(coordinate_list) - 1
    if "context" in heritage:
        for context in heritage['context']:
            coordinate_list[keyword_index]['child'].append(len(coordinate_list))
            coordinate_list = hereditary_to_ui_coordinates(context, coordinate_list)

    return coordinate_list


def prepare_trained_info_context(data):
    field_name = data['field']
    value = data['value']
    left = data['left']
    top = data['top']
    keyword_variant_used_left = data['keyword_variant_used_2d_left']
    keyword_variant_used_top = data['keyword_variant_used_2d_top']
    field = {'field': field_name}
    if left and 'keyword' in left:
        field['keyword'] = left['keyword']['word']
    else:
        field['keyword'] = ''

    if 'word' in value:
        field['value'] = value['word']
    else:
        field['value'] = ''

    field['coordinates'] = []
    page_no = value['page']

    field['page'] = page_no
    if value:
        value_coords = convert_coord_for_ui(value, page_no)
        logging.info(f"DEBUG 10.3.3.7.1 function name : convert_coord_for_ui input: value={value}, page_no={page_no}  "
                                     f"output: value_coords={value_coords}")
        value_coords['dropdown_value'] = parameters['METHOD_USED_2D']
        value_coords['child'] = []

        coordinate_list = []
        value_coords['id'] = len(coordinate_list)
        coordinate_list.append(value_coords)
        if left:
            logging.info(f'Entering if left')
            coordinate_list[0]['child'].append(len(coordinate_list))
            coordinate_list = hereditary_to_ui_coordinates(left, coordinate_list)
            logging.info(f"DEBUG 10.3.3.7.2 function name : hereditary_to_ui_coordinates input: left={left}  "
                                     f"output: coordinate_list={coordinate_list}")

        if top:
            logging.info(f'Entering if top')
            coordinate_list[0]['child'].append(len(coordinate_list))
            coordinate_list = hereditary_to_ui_coordinates(top, coordinate_list)
            logging.info(f"DEBUG 10.3.3.7.2 function name : hereditary_to_ui_coordinates input: top={top}  "
                                     f"output: coordinate_list={coordinate_list}")

        field['coordinates'] = coordinate_list

    field['validation'] = ''
    field['keycheck'] = True
    field['method_used'] = parameters['METHOD_USED_2D']
    field['keyword_variant_used_2d_top'] = keyword_variant_used_top
    field['keyword_variant_used_2d_left'] = keyword_variant_used_left
    field['used_2d_left_top_words'] = data.get('used_2d_left_top_words', None)

    return field


def get_table_lines(tables_area, wand_pdf):
    """
    Author : AKshat Goyal
    Args:
        tables_area:
        wand_pdf:

    Returns:

    """
    table_lines = []
    # logging.debug(f'wand_pdf - {wand_pdf}')
    if not wand_pdf:
        return table_lines
    for idx, page in enumerate(tables_area):
        img = wand_pdf[idx]
        try:
            w, h = img.shape[:2]
        except Exception as e:
            # logging.warning(f"not able to read - {imgpath}")
            img = np.zeros((1000, 1000, 3), np.uint8)
            w, h = img.shape[:2]

        rf = parameters['default_img_width'] / int(h)
        img = cv2.resize(img, (0, 0), fx=rf, fy=rf)
        page_lines = []
        rf = 1
        for area in page:
            if not area:
                continue
            top = area['top']
            bottom = area['bottom']
            table_img = img[int(top):int(bottom), :]
            hor, ver = get_cv_lines_double(table_img, rf, scale=LINES_SCALE)
            logging.info(f'## DEBUG 7.4.1 function name : get_cv_lines_double ,\n  input : table_img={table_img} \n rf={rf} \n scale={LINES_SCALE} \n output : hor={hor} \n ver={ver}')

            hor = [line + top for line in hor]

            lines = {'horizontal': hor, 'vertical': ver}
            page_lines.append(lines)
        table_lines.append(page_lines)

    return table_lines


def history_builder(keyword, contexts, history):
    if not contexts:
        return
    if keyword in history:
        return

    history[keyword] = {}
    for context in contexts:
        if not context:
            continue
        position = context.get('position', None)
        if not position:
            continue

        context_word = context.get('keyword', {}).get('word', None)

        if not context_word:
            continue

        context_word = clean_word(context_word)
        if position not in history[keyword]:
            history[keyword][position] = {}

        if context_word in history[keyword][position]:
            history[keyword][position][context_word] += 1
        else:
            history[keyword][position][context_word] = 1

        context_of_context = context.get('context', None)

        if context_of_context:
            history_builder(context_word, context_of_context, history)


def clean_word(word):
    words = word.split()
    word_list = [remove_all_except_al_num(sinlge_word) for sinlge_word in words]
    word = ' '.join(word_list)
    return word


def history_convertor_2D(context_hierarchy):
    keywords = context_hierarchy['2d_boundary']
    left = keywords.get('left', None)
    top = keywords.get('top', None)

    if not left or not top:
        return [], [], {}

    field_variation = left.get('keyword', {}).get('word', None)
    header_keywords = top.get('keyword', {}).get('word', None)

    if not field_variation or not header_keywords:
        return [], [], {}

    field_variation = [(field_variation, 1)]  # (keyword, weight)
    header_keywords = [header_keywords]

    history = {}

    left_word = clean_word(field_variation[0][0])
    top_word = clean_word(header_keywords[0])
    logging.info(f"DEBUG 10.3.2.1 function name : clean_word for left word input: field_variation for left word={field_variation[0][0]}, top word input: field_variation for top word={header_keywords[0]} output: left_word = {left_word}, top_word = {top_word}")
    history_builder(left_word, left.get('context', None), history)
    logging.info(f'DEBUG 10.3.2.2 function name : history_builder for left word input: left_word={left_word}, context for left= {left.get("context", None)}, history={history}, output={history_builder(left_word, left.get("context", None), history)}')
    history_builder(top_word, top.get('context', None), history)
    logging.info(f'DEBUG 10.3.2.2 function name : history_builder for top word input: top_word={top_word}, context for top= {top.get("context", None)}, history={history}, output={history_builder(top_word, top.get("context", None), history)}')
    

    logging.debug(f'field_variation - {field_variation}, header_keywords - {header_keywords}, history - {history}')
    return field_variation, header_keywords, history


def history_convertor_1D(context_hierarchy):
    keyword = context_hierarchy.get('keyword', None)

    if not keyword:
        return [], {}

    field_variation = keyword.get('keyword', {}).get('word', None)

    if not field_variation:
        return [], {}

    field_variation = [(field_variation, 1)]  # (keyword, weight)

    history = {}

    keyword_clean = clean_word(field_variation[0][0])
    logging.info(f"DEBUG 10.3.2.1 function name : clean_word input: field_variation ={field_variation[0][0]}, output: keyword_clean = {keyword_clean} ")

    history_builder(keyword_clean, keyword.get('context', None), history)
    logging.info(f'DEBUG 10.3.2.2 function name : history_builder input: keyword_clean={keyword_clean}, context = {keyword.get("context", None)}, history={history}, output={history_builder(keyword_clean, keyword.get("context", None), history)}')

    logging.debug(f'field_variation - {field_variation}, history - {history}')
    return field_variation, history


def get_multi_dimension(direction):
    direction_dict = {
        'ENE': ['ESE', 'ENE', 'SSE'],
        'NNE': ['ENE', 'NNE', 'NNW'],
        'NNW': ['NNE', 'NNW', 'WNW'],
        'WNW': ['NNW', 'WNW', 'WSW'],
        'WSW': ['WNW', 'WSW', 'SSW'],
        'SSW': ['WSW', 'SSW', 'SSE'],
        'SSE': ['SSW', 'SSE', 'ESE'],
        'ESE': ['SSE', 'ESE', 'SSE']
    }

    if direction in direction_dict:
        return direction_dict[direction]
    else:
        logging.error(f'why is direction - {direction} not in direction dict')
        return [direction]


def get_rel_info_advanced(box1, box2, direction=None, multi_direction=False):
    """
    line drawn from box1 to box2
    box1 : key
    box2 : value
    """
    if 'left' not in box1 or 'left' not in box2:
        return {}

    # remember top and bottom are inverse in computer graphics
    mid1 = (box1['left'] + int(abs(box1['left'] - box1['right']) / 2),
            -(box1['top'] + int(abs(box1['top'] - box1['bottom']) / 2)))
    mid2 = (box2['left'] + int(abs(box2['left'] - box2['right']) / 2),
            -(box2['top'] + int(abs(box2['top'] - box2['bottom']) / 2)))

    dist = math.hypot(mid2[0] - mid1[0], mid2[1] - mid1[1])
    try:
        angle = math.atan2((mid2[1] - mid1[1]), (mid2[0] - mid1[0]))
        angle = math.degrees(angle)
    except:
        # value at top
        if mid2[1] < mid1[1]:
            angle = -90
        else:
            angle = 90

    # N - north, E - east, W - west, S  -sount
    if direction:
        direction_return = ''
        if int(angle) in range(0, 45):
            direction_return = 'ENE'
        elif int(angle) in range(45, 90):
            direction_return = 'NNE'
        elif int(angle) in range(90, 135):
            direction_return = 'NNW'
        elif int(angle) in range(135, 181):
            direction_return = 'WNW'
        elif int(angle) in range(-180, -135):
            direction_return = 'WSW'
        elif int(angle) in range(-135, -90):
            direction_return = 'SSW'
        elif int(angle) in range(-90, -45):
            direction_return = 'SSE'
        elif int(angle) in range(-45, 0):
            direction_return = 'ESE'

        if multi_direction:
            return get_multi_dimension(direction_return)
        else:
            return direction_return

    # logging.debug({'dist':dist,'angle':angle})
    return {'dist': dist, 'angle': angle}


def ocrDataLocal_special(T, L, R, B, ocrData):
    '''
        Parameters : Boundaries of Scope
        Output     : Returns that part of ocrdata which is confined within given boundaries
    '''

    ocrDataLocal = []
    for data in ocrData:
        if percentage_inside([L, R, B, T], [data['left'], data['right'], data['bottom'], data['top']]) > parameters[
            'overlap_threshold']:
            ocrDataLocal.append(data)
    return ocrDataLocal


def merge_fields(box_list, page_number=0):
    '''
    Merge 2 or more words and get combined coordinates
    '''
    if box_list and type(box_list[0]) is dict:
        min_left = min([word['left'] for word in box_list])
        min_top = min([word['top'] for word in box_list])
        max_right = max([word['right'] for word in box_list])
        max_bottom = max([word['bottom'] for word in box_list])
        min_confidence = min([word['confidence'] for word in box_list])

        max_height = max_bottom - min_top
        total_width = max_right - min_left
        word = ' '.join([word['word'] for word in box_list])

        return {'height': max_height, 'width': total_width, 'y': min_top, 'x': min_left, 'right': max_right,
                'word': word.strip(), 'page': page_number, 'left': min_left, 'top': min_top, 'bottom': max_bottom,
                'confidence': min_confidence}
    else:
        return {}


def get_pre_process_char(pre_process_char, ocr_data, page_no):
    """
    """
    if not pre_process_char:
        logging.info(f'pre prossesing not done, VERY BADDDDD!!!')
        char_index_list, haystack = convert_ocrs_to_char_dict_only_al_num(ocr_data[page_no])
        logging.info(f'## DEBUG 11.2.1.1 function_name: convert_ocrs_to_char_dict_only_al_num input: {ocr_data[page_no]}, output: char_index_list={char_index_list}, haystack={haystack}')

    else:
        try:
            char_index_list, haystack = pre_process_char[page_no]
        except:
            logging.warning(f'error in getting pre process char')
            char_index_list, haystack = convert_ocrs_to_char_dict_only_al_num(ocr_data[page_no])
            logging.info(f'## DEBUG 11.2.1.1 function_name: convert_ocrs_to_char_dict_only_al_num input: {ocr_data[page_no]}, output: char_index_list={char_index_list}, haystack={haystack}')

    return char_index_list, haystack

def sort_ocr_top(a, b):
    logging.info(f'## DEBUG 4.3 function name : sort_ocr_top input : {a} , {b}')
    if a['top'] < b['top']:
        return -1
    elif a['top'] > b['top']:
        return 1
    return 0

def sort_ocr_left(a, b):
    if a['left'] < b['left']:
        return -1
    elif a['left'] > b['left']:
        return 1
    return 0

def sort_ocr_vert(a, b):
    # even if there is a some error between pixel between boxes we will accomodate it
    # lets say a box should be above the box if some part of bottom is below the top then if it is within room of error
    # we will take it
    room_for_error = 4
    # logging.info(f'## DEBUG 4.1 function name : sort_ocr_vert input : a , b')
    # a is above b
    if a['bottom'] < b['top']:
        return -1
    # a is above b with room for error
    # we check if a and b are atleast bigger than room of error iteself
    elif a.get('height', 0) / room_for_error >= 1.5 and b.get('height', 0) / room_for_error >= 1.5 and abs(
            b['top'] - a['bottom']) < room_for_error:
        return -1

    # b is above a
    elif b['bottom'] < a['top']:
        return 1
    # b is above a within room for error
    elif a.get('height', 0) / room_for_error >= 1.5 and b.get('height', 0) / room_for_error >= 1.5 and abs(
            a['top'] - b['bottom']) < room_for_error:
        return 1
    else:
        left_score = sort_ocr_left(a, b)
        # logging.info(f'## DEBUG 4.2 function name : sort_ocr_left input :  output : left_score')
        if  left_score == 0:
            return sort_ocr_top(a, b)
        else:
            return left_score

def sort_ocr(ocr_data):
    from  functools import cmp_to_key
    return sorted(ocr_data, key=cmp_to_key(sort_ocr_vert))


def validate(output_, ocr_fields_dict, standard_format):
    from dateutil.parser import parse
    for field_name, field_value in output_.items():
        field_suspicious_check = False

        if not field_value:
            continue

        if 'suspicious' in field_value:
            field_suspicious_check = True

        # if 'date' in field_name.lower():
        #     if ocr_fields_dict.get(field_name, {}).get('pattern_conversion', ''):
        #         standard_format = ocr_fields_dict[field_name].get('pattern_conversion', '')
        #     if field_value is not None or field_value:
        #         new_field_value = field_value
        #         raw_field_value = field_value.replace('suspicious', '').strip()
        #
        #         try:
        #             parsed_date = parse(raw_field_value, fuzzy=True, dayfirst=True)
        #         except:
        #             logging.exception(f'Error occured while parsing date field `{field_name}`:`{field_value}`.')
        #             parsed_date = None
        #         if parsed_date is not None:
        #
        #             if 'suspicious' in field_value:
        #                 new_field_value = 'suspicious' + parsed_date.strftime(standard_format)
        #             else:
        #                 new_field_value = parsed_date.strftime(standard_format)
        #         output_[field_name] = new_field_value.replace('suspicious', '')
        if "invoice number" in field_name.lower() and output_[field_name]:
            try:
                output_[field_name] = output_[field_name].replace(' ', '')
            except:
                pass
        if "gstin" in field_name.lower() and field_value:
            pattern = r"\d{2}[a-zA-Z]{5}\d{4}[a-zA-Z]{1}\d{1}[a-zA-Z]{1}\w"
            try:
                valid_gstin = re.findall(pattern, field_value.replace('suspicious', '').replace(' ', ''))[-1]
                if valid_gstin:
                    output_[field_name] = valid_gstin
            except:
                output_[field_name] = field_value.replace(' ', '') + 'suspicious'

        # try:
        #     field_value = field_value.replace(' ', '')
        #     field_value = field_value.strip()
        # except:
        #     pass
        # if "po number" in field_name.lower():
        #     if field_value:
        #         if field_suspicious_check:
        #             field_value = field_value.replace('suspicious', '')
        #             try:
        #                 field_value = field_value.replace(':', '').replace('.', '')
        #                 output_[field_name] = field_value[:10] + 'suspicious'
        #             except Exception as e:
        #                 output_[field_name] = field_value + 'suspicious'
        #         else:
        #             try:
        #                 output_[field_name] = field_value[:10]
        #             except Exception as e:
        #                 output_[field_name] = field_value + 'suspicious'

        # if 'amount' in field_name.lower():
        #     if field_suspicious_check:
        #         try:
        #             output_[field_name] = str(
        #                 float(''.join(re.findall(r'[0-9\.]', field_value.replace('suspicious', ''))))) + 'suspicious'
        #         except:
        #             if field_value:
        #                 output_[field_name] = field_value.replace(' ', '') + 'suspicious'
        #     else:
        #         try:
        #             output_[field_name] = str(
        #                 float(''.join(re.findall(r'[0-9\.]', field_value.replace('suspicious', '')))))
        #         except:
        #             if field_value:
        #                 output_[field_name] = field_value.replace(' ', '') + 'suspicious'

    return output_


def get_pre_process_char_space(ocr_data):
    """
    this is so that we can regex match words, whcih without spaces is darn difficult
    """
    pre_processed_char = []
    for page in ocr_data:
        pre_processed_char.append(convert_ocrs_to_char_dict_only_al_num_space(page))
    return pre_processed_char


def get_file_trace_train(file_trace):
    field_data = defaultdict(set)
    for case, data in file_trace.items():
        for _, field in data.get('fields', {}).items():
            if 'field' not in field:
                continue
            field_data[field['field']].add(case)
    return field_data


def case_in_file_trace(file_trace, case_id, field):
    if field in file_trace:
        if case_id and case_id not in file_trace[field]:
            return False
    else:
        return False
    return True


def check_regex_with_error(string, pattern, error_threshold=2):
    # to get fuzzt count (subs, insert, del)
    # re.fullmatch(r"(string){e<120}", string).fuzzy_counts
    # logging.debug(f'regex match pattern - {pattern}, string - {string}')
    if not pattern:
        return True
    if regex.fullmatch(r"(%s)" % (pattern), string):
        return True
    elif error_threshold and regex.fullmatch(r"(%s){e<=%s}" % (pattern, error_threshold), string):
        return True
    else:
        return False


def regex_generator(string):
    regex_string = ''
    for char in string:
        if char.isdigit():
            regex_string += '\d'
        elif char.isalpha():
            regex_string += '[a-zA-Z]'
        elif char == '*':
            pass
        else:
            regex_string += char

    return regex_string


def regex_match(pattern, ocr_data, error_threshold_per):
    matched = []
    for data in ocr_data:
        error_threshold = int(len(data['word']) * error_threshold_per)
        if check_regex_with_error(data['word'], pattern, error_threshold):
            matched.append(data)
    return matched


def check_multiple(value, ocr_data):
    pattern = regex_generator(value['word'])

    T, L, R, B = value['top'] - parameters['MULTIPLE_VALUE_TOP'], value['left'] - parameters['MULTIPLE_VALUE_LEFT'], value['right'] + parameters['MULTIPLE_VALUE_RIGHT'], value[
        'bottom'] + parameters['MULTIPLE_VALUE_BOTTOM']
    T, L, R, B = [max(coord, 0) for coord in (T, L, R, B)]

    ocr_data_filtered = ocrDataLocal(T, L, R, B, ocr_data)
    ocr_data_filtered = [data for data in ocr_data_filtered if not (data['word'] == value['word'] or percentage_inside_lazy(data, value) or percentage_inside_lazy(value, data))]
    return regex_match(pattern, ocr_data_filtered, int(parameters['MULTIPLE_VALUE_ERROR_THRES']))

## This functions added below are used for prediction code - vahida
def modify_data(input_dict):
    """
    This function is used to manipulate output data from prediction so that it is similar to the data we
      typically use.
      output_data_format={'keys':{'key':{'top':,'bottom':,'left':,'right':}},'values':{'top':,'bottom':,'left':,'right':}}

    """
    input_dict=input_dict
    logging.info(f'raw prediction data before manipulation:{input_dict}')
    
    output_dict = {
        'keys': {},
        'values': {}
    }

    for key, value in input_dict.items():
        if key.startswith('key_'):

            actual_key = key[4:]
            if actual_key in input_dict:
                output_dict['keys'][key] = {
                    'top': value[1],
                    'bottom': value[3],
                    'left': value[0],
                    'right': value[2]
                }
                output_dict['values'][actual_key] = {
                    'top': input_dict[actual_key][1],
                    'bottom': input_dict[actual_key][3],
                    'left': input_dict[actual_key][0],
                    'right': input_dict[actual_key][2]
                }
        else:
            key_with_key_ = 'key_' + key
            if key_with_key_ in input_dict:
                output_dict['keys'][key_with_key_] = {
                    'top': input_dict[key_with_key_][1],
                    'bottom': input_dict[key_with_key_][3],
                    'left': input_dict[key_with_key_][0],
                    'right': input_dict[key_with_key_][2]
                }
                output_dict['values'][key] = {
                    'top': value[1],
                    'bottom': value[3],
                    'left': value[0],
                    'right': value[2]
                }

    return output_dict

def ocrDataLocal_special_parsed(T, L, R, B, ocrData):
    '''
        Parameters : Boundaries of Scope
        Output     : Returns that part of ocrdata which is confined within given boundaries
    '''

    # logging.info(f" ########## ocr parsed data got is {ocrData}")
    ocrDataLocal = []
    # for page_no,sent in ocrData.items():
    for sent in ocrData:
        if percentage_inside([L, R, B, T], [sent['left'], sent['right'], sent['bottom'], sent['top']]) > parameters[
            'overlap_threshold']:
            new_value={'word':sent['word'],'confidence':sent['confidence'], 'left':sent['left'], 'right':sent['right'], 'top': sent['top'], 'bottom': sent['bottom']}
            logging.info(f" new value is {new_value}")
            ocrDataLocal.append(new_value)
    return ocrDataLocal


def get_value_highlights(predicted_data,case_id,tenant_id,ocr_data,page_no):
    """ 
       input :  predicted_data => dictionary with predicted keys and values
       functionality : get the value from ocr_parsed_data with the help of predicted labels from predicted data using ocrDataLocal_special_parsed function
       output : returns the values and highlight co-ordinates 
    """
    logging.info(f"in get_values_highlights fn, predicted_data is {predicted_data} page_num is {page_no}")
    output_dict={}
    highlights={}
    try:
        predicted_data=json.loads(predicted_data)
    except:
        pass
    for field_name, value_box in predicted_data.items():
        value_ocr = ocrDataLocal_special_parsed(value_box['top'], value_box['left'], value_box['right'], value_box['bottom'],ocr_data[page_no])
        field_highlight={}
        # logging.info(f" #### value ocr got is {value_ocr}")
        if value_ocr:
            value_ocr[0]['page']=page_no
            output_dict[field_name]=value_ocr[0]['word']
            highlight = merge_fields(value_ocr, page_no)
            field_highlight[field_name]=highlight
            highlights.update(field_highlight)
        else:
            logging.info(f"########### shitttttttt value not found ")
    return output_dict,highlights

def update_highlights(tenant_id,document_id,highlights):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    query=f"select * from ocr where document_id='{document_id}'"
    highlight_df=extraction_db.execute(query)
    highlight=highlight_df.to_dict(orient='records')
    highlight=highlight[0]['highlight']
    highlight=json.loads(highlight)

    if highlight:
        highlight.update(highlights)
        highlight=json.dumps(highlight)
        query = "UPDATE `ocr` SET `highlight`= %s WHERE `document_id` = %s"
        params = [highlight, document_id]
        extraction_db.execute(query, params=params)
    else:
        word_highlight=json.dumps(highlights)
        query = "UPDATE `ocr` SET `highlight`= %s WHERE `document_id` = %s"
        params = [word_highlight, document_id]
        extraction_db.execute(query, params=params)
    return "true"


def remove_lock_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".lock"):
            lock_file_path = os.path.join(directory, filename)
            os.remove(lock_file_path)
            logging.info(f"Removed: {lock_file_path}")
    return True

def insert_into_response_model(values,highlights,tenant_id,document_id,case_id,file_type,api):
    db_config['tenant_id']=tenant_id
    template_db=DB("template_db",**db_config)
    if api=="prediction_api":
        query = "insert into `response_data_models` (`case_id`, `document_id`,`layoutlm_response`,`file_type`) values (%s, %s,%s,%s)"
        params = [case_id,document_id,values,file_type]
        template_db.execute_(query,params=params)
    else:
        # query = "update `response_data_models` set `centroid_response`=%s where `case_id` = %s and `document_id`= %s"
        # query = "INSERT OR REPLACE INTO `response_data_models` (`case_id`, `document_id`,`centroid_response`,`file_type`) VALUES (%s, %s,%s,%s)"
        values=json.dumps(values)
        query = "INSERT INTO `response_data_models` (`case_id`, `document_id`,`centroid_response`,`file_type`) VALUES (%s, %s,%s,%s) ON DUPLICATE KEY UPDATE  `case_id` = '{case_id}', `document_id` = '{document_id}', `centroid_response`='{values}',`file_type`='{file_type}'"
        params = [case_id,document_id,values,file_type]
        print(f" ######### qeury is {query}")
        template_db.execute_(query,params=params)
    
    return True


def get_fields_model_wise(tenant_id,file_type):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    fild_mdl_dict={"layoutlm":[],"centroid":[]}
    qry=f"SELECT * FROM `fields_training` where `file_type` like '%%{file_type}%%'"
    fields_df=extraction_db.execute_(qry)
    fields_df=fields_df.to_dict(orient='records')
    # print(fields_df)
    for dict_ in fields_df:
        if dict_['model']=='layoutlm':
    #         print(f"dict is {dict_['model']}")
            fild_mdl_dict['layoutlm'].append(dict_['unique_name'])
        elif dict_['model']=='centroid':
    #         print(f"dict is {dict_['model']}")
            fild_mdl_dict['centroid'].append(dict_['unique_name'])
        else:
            pass

#     print(f"###### final fild_mdl_dict is {fild_mdl_dict}")
    return fild_mdl_dict

def update_models_into_ocr(tenant_id,case_id,response_df,model_wise_flds):
    update_ocr_data={}
    lm_flds=model_wise_flds['layoutlm']
    cen_flds=model_wise_flds['centroid']
    response_df=response_df.to_dict(orient='records')
    
    try:
        lm_res=ast.literal_eval(response_df[0]['layoutlm_response'])
        cen_res=ast.literal_eval(response_df[0]['centroid_response'])
    except:
        lm_res=json.loads(response_df[0]['layoutlm_response'])
        cen_res=json.loads(response_df[0]['centroid_response'])
    print(f" ####### lm_res is {lm_res} \n cen_rs is {cen_res}")
    
    try:
        for fild1 in lm_flds:
            try:
                if fild1 in lm_res:
                    update_ocr_data[fild1]=lm_res[fild1]
                else:
                    update_ocr_data[fild1]=cen_res[fild1]
            except Exception as e:
                continue   
        for fild2 in cen_flds:
            try:
                if fild2 in cen_res:
                    update_ocr_data[fild2]=cen_res[fild2]
                else:
                    update_ocr_data[fild2]=lm_res[fild2]
            except Exception as e:
                continue
    except Exception as e:
        pass
    return update_ocr_data

def update_append_highlights(word_highlights,tenant_id,case_id):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction', **db_config)
    query=f"select * from ocr where case_id='{case_id}'"
    highlight_df=extraction_db.execute(query)
    highlight=highlight_df.to_dict(orient='records')
    highlight=highlight[0]['highlight']

    if highlight:
        highlight=json.loads(highlight)
        highlight.update(word_highlights)
        highlight=json.dumps(highlight)
    else:
        highlight=json.dumps(word_highlights)
    return highlight
