import os 
import base64
import requests
from flask import Flask
from flask_cors import CORS

try:
    from app import app
except Exception:
    app = Flask(__name__)
    CORS(app)

try:
    import app.xml_parser_sdk as xml_parser_sdk
except Exception:
    import xml_parser_sdk as xml_parser_sdk

from ace_logger import Logging

logging = Logging()



def ocr_sdk(file_path):
    """
    Author : Akshat Goyal
    """
    try:

        files_data = {'file': open(file_path, 'rb')}
        logging.info(files_data)
        url = os.environ['ABBYY_URL']
        logging.info("===========")
        logging.debug(url)
        response = requests.post(url, files=files_data, timeout=1000)
        xml_string=''
        logging.debug(type(response))
        sdk_output = response.json()
        logging.debug(type(sdk_output))
        logging.debug(sdk_output.keys())
        if 'blob' in sdk_output:
            pdf = base64.b64decode(sdk_output['blob'])
            with open(file_path, 'wb') as f:
                f.write(pdf)
        else:
            logging.info('no blob in sdk_output')

        # shutil.copyfile(file_path, file_path+'_1')
        # os.remove(file_path)
        # os.rename(file_path+'_1', file_path)

        # shutil.copyfile(file_path, 'training_ui/' + file_parent_input + file_name)
        if len(sdk_output['xml_string'])>0:
            xml_string = sdk_output['xml_string'].replace('\r', '').replace('\n', '').replace('\ufeff', '')
        else:
            logging.info("CHECK ABBYY COUNT")
    except:
        xml_string = None
        message = f'Failed to OCR {file_path} using SDK'
        logging.exception(message)

    return xml_string

def ocr_selection(file_path):
    ocr_word = []
    ocr_sen={}
    rotation=[]
    xml_string=''
    dpi_page = []
    try:
        xml_string = ocr_sdk(file_path)
    except Exception as e:
        logging.exception(f'Error runnung OCR. Check trace.')
        return ocr_word,ocr_sen,dpi_page,rotation,xml_string

    try:
        ocr_word,ocr_sen, dpi_page,rotation= xml_parser_sdk.convert_to_json(xml_string)
        
    except Exception as e:
        logging.exception(f'Error parsing XML. Check trace.')
    return ocr_word,ocr_sen,dpi_page,rotation,xml_string



def get_ocr(file_path):
    """
    Author : Aishwarya Jairam
    """
    logging.info(f'Processing file {file_path}')
    xml_string=''

    ocr_word,ocr_sen, dpi_page,rotation,xml_string = ocr_selection(file_path)

    return ocr_word, ocr_sen, dpi_page,rotation,xml_string


