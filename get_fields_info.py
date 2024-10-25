import json,sys
from app.get_fields_info_utils import *

class DevNull(object):
    def write(self, arg):
        pass
    def flush(self):
        pass


def get_fields_info(ocr_data,highlight,field_data):
    all_fields_data = {}
    
    for field,highlight_data in highlight.items():
        try:
            field_info = {}
            keyword = field_data[field]['keyword']
            page_no = int(field_data[field]['page'])
            

            if highlight_data:
                highlight_data['top'] = highlight_data['y']
                highlight_data['left'] = highlight_data['x']
                highlight_data['bottom'] = highlight_data['y']+highlight_data['height']
                page_no = int(highlight_data['page'])
                
                if keyword:
                    try:
                        keyword_meta = needle_in_a_haystack(keyword,ocr_data[page_no],highlight_data)
                        
                    except:
                        keyword_meta = {}
                    box_list = [highlight_data,keyword_meta]
                else:
                    box_list = [highlight_data]
                     
                combined_box = merge_fields(box_list,page_no)
                
                
                field_info['box'] = combined_box
                field_info['box_value'] = keyword+' '+highlight_data['word']
                field_info['keyword'] = keyword
                field_info['value'] = highlight_data['word']
                try:
                    validation = field_data[field]['validation']
                except:
                    validation = ''
                field_info['validation'] = validation
            elif not highlight_data and keyword:
                try:
                    keyword_meta = needle_in_a_haystack(keyword,ocr_data[page_no],field_data[field]['scope'])
                    keyword_meta['page'] = page_no
                    field_info['box_value'] = keyword
                    field_info['keyword'] = keyword
                    field_info['value'] = ''
                    field_info['box'] = keyword_meta
                except Exception as e:
                    print('Failed to fetch the keyword')
            if field_info:
                  
                all_fields_data[field] = field_info
            
        except Exception as e:
            print('Error in sending fields info while retraining',e)
    return all_fields_data
