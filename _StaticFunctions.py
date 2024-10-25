try:
    import Lib
except Exception:
    from . import Lib

import ast
from types import coroutine
import pandas as pd
import numpy as np
from datetime import date,timedelta,datetime
import logging
import json
import ntpath
import re

import os
import operator
import calendar
from dateutil import parser
from dateutil.relativedelta import relativedelta
import re
from difflib import SequenceMatcher
from dateutil.parser import parse
from dateutil.relativedelta import *
from babel.numbers import format_decimal
# from word2number import w2n 
from app.db_utils import DB

try:
    from ace_logger import Logging
    logging = Logging()
except Exception:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG)

CANNOT_FIND_MATCH_MESSAGE = "cannot find match"
 
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}

__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

@register_method
def evaluate_static(self, function, parameters):
    if function == 'do_assign':
        return self.do_assign(parameters)
    if function == 'do_assign_q':
        return self.do_assign_q(parameters)
    if function == 'do_assign_table':
        return self.do_assign_table(parameters)
    if function == 'do_date_compare':
        return self.do_date_compare(parameters)
    if function == 'CompareKeyValue':
        return self.doCompareKeyValue(parameters)
    if function == 'do_get_length':
        return self.do_get_length(parameters)
    if function == 'do_get_range':
        return self.do_get_range(parameters)
    if function == 'do_amount_compare':
        return self.do_amount_compare(parameters)
    if function == 'do_select':
        return self.do_select(parameters)
    if function == 'do_select_all':
        return self.do_select_all(parameters)
    if function == 'do_transform':
        return self.do_transform(parameters)
    if function == 'do_count':
        return self.do_count(parameters)
    if function == 'do_contains':
        return self.do_contains(parameters)
    if function == 'do_produce_data':
        return self.do_produce_data(parameters)
    if function == 'dodue_date_generate':
        return self.dodue_date_generate(parameters)
    if function == 'bankdodue_date_generate':
        return self.bankdodue_date_generate(parameters)
    if function == 'Get_holidays_fromdatabase':
        return self.get_holidays_fromdatabase(parameters)
    if function == 'dosat_and_sun_holidays':
        return self.dosat_and_sun_holidays(parameters)
    if function == 'do_contains_ucic':
        return self.do_contains_ucic(parameters)
    if function == 'do_date_parsing':
        return self.do_date_parsing(parameters)
    if function == 'DateParsingMarch':
        return self.do_date_parsingMarch(parameters)
    if function == 'do_split':
        return self.do_split(parameters)
    if function == 'do_return':
        return self.do_return(parameters)
    if function == 'do_regex_columns':
        return self.do_regex_columns(parameters)
    if function == 'do_alpha_num_check':
        return self.do_alpha_num_check(parameters)
    if function == 'do_date_transform':
        return self.do_date_transform(parameters)
    if function == 'do_partial_match':
        return self.do_partial_match(parameters)
    if function == 'FileManagerUpdate':
        return self.doFileManagerUpdate(parameters)
    if function == 'do_round':
        return self.do_round(parameters)
    if function == 'do_contains_string':
        return self.do_contains_string(parameters)
    if function == 'do_alnum_num_alpha':
        return self.do_alnum_num_alpha(parameters)
    if function == 'do_regex':
        return self.do_regex(parameters)
    if function == 'do_round':
        return self.do_round(parameters)
    if function == 'amount_compare':
        return self.amount_compare(parameters)
    if function == 'do_produce_data':
        return self.do_produce_data(parameters)
    if function == 'do_append_db':
        return self.do_append_db(parameters)
    if function == 'do_partial_compare':
        return self.do_partial_compare(parameters)
    if function == 'do_get_date_time':
        return self.do_get_date_time(parameters)
    if function == 'do_sum':
        return self.do_sum(parameters)
    if function == 'do_date_increment':
        return self.do_date_increment(parameters)
    if function == 'do_nt_path_base':
        return self.do_nt_path_base(parameters)
    if function == 'do_check_date':
        return self.do_check_date(parameters)
    if function == 'DateCheck':
        return self.doDateCheck(parameters)
    if function == 'do_date_parser':
        return self.do_date_parser(parameters)
    if function == 'do_amount_syntax':
        return self.do_amount_syntax(parameters)
    if function == 'do_contains_master':
        return self.do_contains_master(parameters)
    if function == 'do_table_error_messages':
        return self.do_table_error_messages(parameters)
    if function == 'do_date_conversion':
        return self.do_date_conversion(parameters) 
    if function == 'do_user_match':
        return self.do_user_match(parameters)
    if function == 'Statusupdate':
        return self.doStatusupdate(parameters)
    if function == 'do_numeric_extract':
        return self.do_numeric_extract(parameters)
    if function == 'do_transform_':
        return self.do_transform_(parameters)
    if function == 'do_contains_string_':
        return self.do_contains_string_(parameters)
    if function == 'do_partial_comparison':
        return self.do_partial_comparison(parameters)
    if function == 'do_contain_string':
        return self.do_contain_string(parameters)
    if function == 'do_queue_percentage':
        return self.do_queue_percentage(parameters)
    if function == 'donumword_to_number_comp':
        return self.donumword_to_number_comp(parameters)
    if function == 'do_not_contain_string':
        return self.do_not_contain_string(parameters)
    if function == 'do_type_conversion':
        return self.do_type_conversion(parameters)
    if function == 'to_lower':
        return self.to_lower(parameters)
    if function == 'do_dates_diff':
        return self.do_dates_diff(parameters)
    if function == 'is_numeric':
        return self.is_numeric(parameters)
    if function == 'duplicateCheck':
        return self.duplicate_check(parameters)
    if function == 'query_and_check':
        return self.query_and_check(parameters)
    if function == 'partially_compare':
        return self.partially_compare(parameters)
    if function == 'do_extra_year':
        return self.do_extra_year(parameters)
    if function == 'get_last_n_chars':
        return self.get_last_n_chars(parameters)
    if function == 'get_next_month_first_date':
        return self.get_next_month_first_date(parameters)
    if function == 'rb_stock_summary_table1':
        return self.rb_stock_summary_table1(parameters)
    if function == 'cons_stock_table':
        return self.cons_stock_table(parameters)
    if function == 'cons_credi_table':
        return self.cons_credi_table(parameters)
    if function == 'month_and_year':
        return self.month_and_year(parameters)
    if function == 'do_validation_params':
        return self.do_validation_params(parameters)
    if function == 'get_month_last_date':
        return self.get_month_last_date(parameters)
    if function == 'get_month_agri_fifteenth':
        return self.get_month_agri_fifteenth(parameters) 
    if function == 'get_data_dict':
        return self.get_data_dict(parameters)
    if function == 'merge_dict':
        return self.merge_dict(parameters)
    if function == 'date_cus':
        return self.date_cus(parameters)
    if function == 'date_out':
        return self.date_out(parameters)
    if function == 'dosummary':
        return self.dosummary(parameters)
    if function == 'dosummary_1':
        return self.dosummary_1(parameters)
    if function == 'assign_value_json':
        return self.dosummary_1(parameters)
    if function == 'checking_files':
        return self.checking_files(parameters)
    
    if function == 'margin_data':
        return self.margin_data(parameters)
    if function == 'add_key_value':
        return self.margin_data(parameters)

    


@register_method
def date_out(self,parameters):
    """Evaluate the date format."""
    d_month = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
               'september', 'october', 'november', 'december', 'jan', 'feb', 'mar', 'apr',
               'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    value = re.sub(r'[^a-zA-Z0-9 ]', ' ', parameters['date_format']).split('(')[0]
    value_parts = re.split(r'[/\-\s]', value)

    final_words = []
    non_alphanumeric = r'[^a-zA-Z0-9]'

    for part in value_parts:
        clean_part = re.sub(non_alphanumeric, '', part)
        if clean_part.lower() in d_month:
            final_words.append(part)
        else:
            alphabetic_part = re.sub(r'[^a-zA-Z]', '', part)
            numeric_part = re.sub(r'\D', '', part)

            if alphabetic_part.lower() in d_month:
                if alphabetic_part and numeric_part:
                    final_words.extend(sorted([alphabetic_part, numeric_part], key=part.index))
                elif numeric_part.isdigit():
                    final_words.append(numeric_part)

    result = '-'.join(final_words)
    return result if result else parameters['date_format']



def compare_strings(string1, string2):
    seq_matcher = SequenceMatcher(None, string1.lower(), string2.lower())
    similarity_ratio = seq_matcher.ratio() * 100
    return similarity_ratio

@register_method
def date_cus(self, parameters):
    logging.info(f"parameters got are {parameters}")
    name_ = parameters['name__']
    logging.info(f"name_ is  {name_}")
    seg_ = parameters['seg']
    logging.info(f"seg_ is  {seg_}")
    master_tables={"Consumer":"consumer_stock_statement","Agri":"agri","RBG":"rbg_stock_statement","WBG":"wbg_consolidated_master"}
    master_customer_column={"Consumer":"Customer Name","Agri":"Client Name","RBG":"Client Name","WBG":"CRN_NAME"}
    try:
        table=master_tables[seg_]
        column=master_customer_column[seg_]
        ocr_db=DB("extraction",**db_config)
        query=f"select * from {table}"
        customer_list = ocr_db.execute_(query)[column].to_list()
        similarity_ratio=0
        out_cus=''
        for name in customer_list:
            string1=re.sub(r'[^a-zA-Z0-9]', '', name).lower()
            string2=re.sub(r'[^a-zA-Z0-9]', '', name_).lower()
            out=compare_strings(string1, string2)
            if out>similarity_ratio:
                out_cus=name
                similarity_ratio=out

        if similarity_ratio>0.98:
            return out_cus
        else:
            return name_

    except Exception:
        return name_

    except KeyError as e:
        logging.error(f"KeyError: {e}. The segment '{seg_}' may not exist in master_tables or master_customer_column.")
        raise
    except DatabaseError as e:
        logging.error(f"DatabaseError: {e}. Failed to execute query or fetch data.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


    
    

@register_method
def do_get_length(self, parameters):
    """Returns the lenght of the parameter value.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'source':'input', 'value':5}
                      
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    try:
        value = len(value)
        logging.info(value)
    except Exception as e:
        logging.error(e)
        logging.error("giving the defalut length 0")
        value = 0
    return str(value)
    
@register_method
def do_user_match(self, parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"},
        'match_ratio' : "85"
    }

    """
    logging.info(f"parameters got are {parameters}")
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = parameters['match_word']
    match_ratio = parameters['match_ratio']
    data = self.data_source[words_table]
    data = pd.DataFrame(data)
    words = list(data[words_column])
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
            if ratio > int(match_ratio) and ratio > max_ratio:
                max_ratio = ratio
                match_got = word
                logging.info(match_got)
        except Exception as e:
            logging.error(self.CANNOT_FIND_MATCH_MESSAGE)
            logging.error(e)

### TO DO
@register_method
def do_produce_data(self, parameters):
    """Updates the data that needs to be sent to next topic via kafka.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'key':{'source':'input', 'value':5},
                        'value': {'source':'input', 'value':5}
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        kafka_key = self.get_param_value(parameters['key'])
        kafka_value = self.get_param_value(parameters['value'])
        # update the self.kafka data
        self.kafka_data[kafka_key] = kafka_value
    except Exception as e:
        logging.error(e)
        logging.error("Unable to send the data that needs to be sent to next topic via kafka.Check rule")
    return True

@register_method
def do_get_range(self, parameters):
    """Returns the parameter value within the specific range.
    Args:
        parameters (dict): The source parameter and the range we have to take into. 
    eg:
       'parameters': {source':'input', 'value':5,
                        'start_index': 0, 'end_index': 4
                    }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Range is the python range kind of (exclusive of the end_index)
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    start_index = parameters['start_index']
    end_index = parameters['end_index']
    try:
        logging.info(f"Value got in given index is : {str(value)[int(start_index): int(end_index)]}")
        return (str(value)[int(start_index): int(end_index)])
    except Exception as e:
        logging.error("some error in the range function")
        logging.error(e)
    return ""


@register_method
def do_sum(self, parameters):
    """retuns 
    'parameters': {source':'input_config', 'table':'ocr', 'column': 'End_date'}
        
    """
    logging.info(f"parameters got to doSum function are : {parameters}")
    input_series = self.get_param_value(parameters)
    logging.info(f"Got series is : {input_series}")
    if input_series is not None:
        try:
            total = input_series.sum(skipna = True)
            logging.info(f"Got total is : {total}")
            return total
        except Exception as e:
            logging.error("Addition of given series got failed")
            logging.error(e)
            return ""
    else:
        return ""

### TO DO
@register_method
def do_select(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    
    try:
        master_data = self.data_source[from_table]
        
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data)
    

    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = lookup['compare_with']
        logging.info(f"value to compare is : {compare_value}")
        master_df = master_df[master_df[lookup_column].astype(str).str.lower() == str(compare_value).lower()]
    master_df = master_df.reset_index(drop=True)

    
    if not master_df.empty:
        try:
            return master_df[column_name_to_select][0] 
        except Exception as e:
            logging.error("error in selecting the required data from the result")
            logging.error(e)
            return ""
    else:
        return ""



@register_method
def do_select_all(self, parameters):
    """Returns all the vlookup values from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions.
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data)

    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        master_df = master_df[master_df[lookup_column].astype(str) == str(compare_value)]
    master_df = master_df.reset_index(drop=True)

    
    if not master_df.empty:
        try:
            return master_df[column_name_to_select] 
        except Exception as e:
            logging.error("error in selecting the required data from the result")
            logging.error(e)
            return ""
    else:
        return ""

@register_method
def do_amount_trimming(self, parameters):
    """Returns the trimmed amount value data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':{
            "value":"500INR"
        }
    """
    value = parameters["value"]

    value = str(value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
    if value != "":
        value = float(value)

    return value



## TO DO
@register_method
def do_transform(self, parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    equation = ''
    logging.info(f"parameters got are {parameters}")
    for element,number_operator in parameters.items() :
        if element == 'param' :
            value = f'{number_operator}'
            value = str(value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        elif element == 'operator' :
            value = f' {number_operator} '
        equation = equation + value
    logging.info(f"equation is : {equation}")
    try:
        equation = (eval(equation))
    except:
        equation = 0
        logging.info("some value is empty in equation,so final value is 0")
    return equation

## TO DO
@register_method
def do_contains(self, parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
    
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':"5"
                        }
            
    """
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = parameters["value"]
    logging.info(f"Value got is : {value}")
    column_values = self.data_source[table_name]
    logging.info(type(column_values),column_values)
    if value in self.data_source[table_name][column_name]:
        return True
    else :
        return False

@register_method
def do_contains_master(self, parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            
    """
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = parameters['value']
    logging.info(value)
    df = pd.DataFrame(self.data_source[table_name])
    logging.info(list(df[column_name]))
    if str(value) in list(df[column_name].astype(str)):
        return True
    else :
        return False

@register_method
def do_count(self, parameters):
    """Returns the count of records from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = lookup['compare_with']
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return len(result_df) # just return the first value of the matches
        except Exception as e:
            logging.error("error in selecting the required data from the result")
            logging.error(e)
            return 0
    else:
        return 0

@register_method
def do_produce_data(self, parameters):
    """Updates the data that needs to be sent to next topic via kafka.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'key':{'source':'input', 'value':5},
                        'value': {'source':'input', 'value':5}
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        kafka_key = self.get_param_value(parameters['key'])
        kafka_value = self.get_param_value(parameters['value'])
        # update the self.kafka data
        self.kafka_data[kafka_key] = kafka_value
    except Exception as e:
        logging.error(e)
        logging.error("Unable to send the data that needs to be sent to next topic via kafka.Check rule")
    return True


######################################### HSBC ##################################################



@register_method
def dodue_date_generate(self, parameters):
    """ 
        Due_Date_generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        duedate_rule = {'rule_type': 'static',
                        'function': 'due_date_generate',
                        'parameters': {'Extended_days':{'source':'input_config','table':"ocr",'column':'Default Extension'}, 
                      }
    }   
    """
    logging.info(f"parameters got are {parameters}")
    holidays = self.get_param_value(parameters["holidays"])
    extended_days = self.get_param_value(parameters["Extended_days"])
    logging.info(f"Extended_days {extended_days}")
    logging.info(holidays)
    extended_days,_ = extended_days.split(" ")
    extended_days = int(extended_days)
    try:
        today = date.today()
        due_date = today + timedelta(days = int(extended_days))
        date_list = [(today + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(extended_days+1)]
        li_dif = [i for i in holidays if i in holidays and i in date_list]
        logging.info(f"Number of holidays are: {len(li_dif)},{due_date}")
        due_date = due_date + timedelta(days = int(len(li_dif)))
        logging.info(f"due_date is : {due_date}")
        while True:
            if str(due_date) not in holidays :
                logging.info(f"due_date2 is : {due_date}")
                return due_date
            else:
                due_date = due_date + timedelta(days = 1)
                logging.info(f"due_date3 is : {due_date}")
    except Exception as e:
        logging.error("> Cannot Generate DueDate_generate ERROR : in dodue_date_generate_function")
        logging.error(e)
        return False

@register_method
def bankdodue_date_generate(self, parameters):
    """
        Bank_Due_Date_generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        duedate_rule = { 'rule_type': 'static',
        'function': 'Bank_due_date_generate',
        'parameters': {'Due_date':{'source':'input_config','table':"ocr",'column':'Due Date(Notice)'}
        }
    }
    """
    holidays = self.get_param_value(parameters["holidays"])
    logging.info(holidays)
    logging.info(f"parameters got are {parameters}")
    due_date = self.get_param_value(parameters["Due_date"])
    receipt_time = self.get_param_value(parameters["Receipt_time"])
    try:
        today = date.today()
        date_format = "%Y-%m-%d"
        a = datetime.strptime(str(today), date_format)
        b = datetime.strptime(str(due_date), date_format)
        diff_days = (b - a).days
        logging.info(f"diff_days are =====> {diff_days}")
        due_date_date = datetime.strptime(str(due_date),"%Y-%m-%d").date()
        if diff_days == 0 or diff_days == -1 or diff_days == 1 or diff_days == 2 or diff_days == 3 :
            due_date1 = date.today()
            todaydate = datetime.now()
            receipt_time = datetime.strptime(receipt_time,"%Y-%m-%d %H:%M:%S")
            today12pm = todaydate.replace(hour=12, minute=0, second=0, microsecond=0)
            if receipt_time > today12pm:
                due_date1 = today + timedelta(days = 1)
            else:
                due_date1 = date.today()
        elif diff_days == 7 :
            due_date1 = due_date_date - timedelta(days = 2)
        elif diff_days == 15 or diff_days == 16 :
            due_date1 = due_date_date - timedelta(days = 3)
        elif diff_days >= 17 :
            due_date1 = today + timedelta(days = 2)
        else:
            due_date1 = due_date_date - timedelta(days = 2)
        logging.info(f"Duedate in Bank_is ===> {due_date1}")

        while True:
            if str(due_date1) not in holidays :
                logging.info(f"Bank_Due_date is ======> {due_date1}")
                return str(due_date1)
            else:

                due_date1 = due_date1 - timedelta(days = 1)

                Due_date1 = Due_date1 - timedelta(days = 1)   

                due_date1 = due_date1 - timedelta(days = 1)

    except Exception as e:
        logging.error("====>Cannot Generate Bankdodue_date_generate ERROR : in Bankdodue_date_generate_function")
        logging.error(e)
        return False

@register_method
def doSat_and_sun_holidays(self, parameters):

    """
    Holidays generation.
    
    Args:
        parameters (dict): The source parameter which includes values that should be checked.

    eg:
        dosat_sun_rule = {'rule_type': 'static',
                        'function': 'dosat_sun_generate',
                        'parameters': {'extended_days':{'source':'input_config','table':"ocr",'column':'Default Extension'}, 
                      }
    }   


    """
    logging.info(f"parameters got are {parameters}")
    
    try:
        year = int(datetime.now().year)
        logging.info(f"year is : {year}")

        # Generate all Sundays
        years1 = []
        dt1 = date(year, 1, 1)
        dt1 += timedelta(days=(6 - dt1.weekday()))  # First Sunday
        while dt1.year == year:
            years1.append(str(dt1))
            dt1 += timedelta(days=7)
        logging.info(f"all_sundays are {years1}")

        # Generate second Saturdays
        years2 = []
        dt2 = date(year, 1, 1)
        dt2 += timedelta(days=(5 - dt2.weekday()))  # First Saturday
        while dt2.year == year:
            if 8 <= dt2.day <= 14:
                years2.append(str(dt2))
            dt2 += timedelta(days=7)
        logging.info(f"all_second_saturdays are {years2}")

        # Generate fourth Saturdays
        years3 = []
        dt3 = date(year, 1, 1)
        dt3 += timedelta(days=(5 - dt3.weekday()))  # First Saturday
        while dt3.year == year:
            if 22 <= dt3.day <= 28:
                years3.append(str(dt3))
            dt3 += timedelta(days=7)
        logging.info(f"all_fourth_saturdays are {years3}")

    except Exception as e:
        logging.error("Cannot Generate Holiday_list ERROR: in sat_sun_holiday_generate_function")
        logging.error(e)
        return False


    holiday_list = years1+years2+years3
    logging.info(holiday_list)
    return holiday_list

    Holiday_list = years1 + years2 + years3
    logging.info(Holiday_list)
    return Holiday_list



@register_method
def get_holidays_fromdatabase(self, parameters):
    logging.info(f"parameters got are {parameters}")
    from_table1 = self.get_param_value(parameters['from_table1'])
    from_column1 = self.get_param_value(parameters['from_column1'])
    sun_sat_holidays_list = self.get_param_value(parameters['sun_sat_holidays'])
    try:
        holidays_df = (self.data_source[from_table1])
        logging.info(holidays_df)
        logging.info("====== @ ===>")
        holidays_df = pd.DataFrame(holidays_df)
        holidays_df[from_column1] = holidays_df[from_column1].astype(str)
        holidays_df[from_column1] =  pd.to_datetime(holidays_df[from_column1],dayfirst=True,errors='coerce').dt.strftime('%Y-%m-%d')
        holidays_list = holidays_df[from_column1].tolist()
        logging.info(holidays_list)
        total_holidays = holidays_list + sun_sat_holidays_list
        logging.info(f"total_holidays are: {total_holidays}")
        return total_holidays
    except Exception as e:
        logging.error(f"==> Error in Adding Holidays ")
        logging.error(e)        

@register_method
def do_contains_ucic(self, parameters):
    """ Returns BOOLEAN value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        { 'rule_type':'static',
        'function': 'Contains_UCIC',
        'parameters' : {
                        'column1_map_id': 'Customer ID',
                        'table_name_acc': 'close_account_dump',
                        'column1_acc_id': 'CUSTOMER_ID',
                        'value1':{'source':'input_config', 'table':'ocr', 'column': 'Mapping Table'},
                        }
        }
        """
    logging.info(f"parameters got are {parameters}")
    column1_map_id =  parameters['column1_map_id']
    table_name_acc =  parameters['table_name_acc']
    column1_acc_id =  parameters['column1_acc_id']
    value_map = self.get_param_value(parameters['value1'])
    logging.info(value_map)
    logging.info(table_name_acc)
    try:
        dict_list = []
        value_map = json.loads(value_map)
        map_dict = [dict_list.append(e) for e in value_map[0]["rowData"]]
        map_df = pd.DataFrame(dict_list)
        df_acc = self.data_source[table_name_acc]
        df_acc = pd.DataFrame(df_acc)
        logging.info(df_acc)
        id_list = list(df_acc[column1_acc_id])
        logging.info(id_list)
        for index, row in map_df.iterrows():
            if row[column1_map_id] in id_list :
                return 'Match Found'
        return 'Match Not Found'       
    except Exception as e:
        logging.error("Error in => do_contains_ucic Function")
        logging.error(e)
        return False


@register_method
def do_contains_string(self, parameters):
    """ Returns true value if the string is present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    word = parameters['word']
    strings_list = parameters['strings_list']
    try:
        for string in strings_list:
            if string in word:
                return True
        return False
    except Exception as e:
        logging.info('===> Error in do_contains_string')
        logging.info(e)
        return False

        
@register_method
def do_append_db(self,parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        assign_table = parameters['assign_table']
        value_to_concate = parameters['assign_value']
        table_key = assign_table['table']
        column_key = assign_table['column']
        concat_value = self.data_source[table_key][column_key]
        logging.info(f'Concat Value is {concat_value}')
    except Exception as e:
        logging.error("Error In doAppend function")
        logging.error(e)
    try:
        
        if concat_value == "" or concat_value == 'NULL' or concat_value == None:
            concated_string = value_to_concate
        else:
            concated_string = concat_value+"\n"+value_to_concate
        
        try:
            self.data_source[table_key][column_key] = str(concated_string)
            if table_key not in self.changed_fields:
                self.changed_fields[table_key] = {}
            self.changed_fields[table_key][column_key] = str(concated_string)
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in doappenddb function for : {column_key}")
            logging.error(e)
        return True
    except Exception as e:
        logging.error("Error In doAppend function")
        logging.error(e)
        return False
        
        
@register_method
def do_date_parsing(self,parameters):
    """Checks whether given date is last day of that month or not, returns true if yes
        parameters:{
            "source":"input_config","table":"ocr","column":"Stock Date"
        }
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_data(parameters)
    try:
        input_date = input_date.replace('.','-')
        input_date = input_date.replace('/','-')
        input_date = input_date.replace(' ','-')
        input_date = input_date.replace('st','')
        input_date = input_date.replace('th','')
    except Exception:
        input_date = input_date
    
    logging.info(f"date got is {input_date}")
    list_31 = ['jan','mar','may','jul','aug','oct','dec','01','03','05','07','08','10','12','january','	march','may','july','august','october','december','1','3','5','7','8']
    list_30 = ['apr','jun','sep','nov','04','06','09','11','april','june','september','november','4','6','9']
    try:
        input_list = input_date.split("-")
        if len(input_list) == 2:
            if input_list[0].lower() in list_31:
                input_date = "31-"+input_date
            elif input_list[0].lower() in list_30:
                input_date = "30-"+input_date
            else:
                feb_last = calendar.monthrange(int(input_list[1]),2)[1]
                input_date = str(feb_last)+"-"+input_date
        logging.info(f"Converted date is {input_date}")
    except Exception as e :
        logging.error("Cannot convert date")
        logging.error(e)

    try:
        input_date = parser.parse(input_date,default=datetime(2019, 10, 3))
        date_list = str(input_date).split("-")
        input_day = date_list[2][0:2]
        input_month = date_list[1]
        input_year = date_list[0]
        logging.info(f"input date is: {input_day}")
        month_last = calendar.monthrange(int(input_year),int(input_month))[1]
        logging.info(f"last day of given month is {month_last}")
        if str(input_day) == str(month_last):
            logging.info(f"given date and month's last date are same")
            return True
        else:
            logging.info(f"given date and month's last date are different")
            return False
    except Exception as e:
        logging.error("Cannot compare two dates")
        logging.error(e)
        return False

@register_method
def do_date_parsing_march(self,parameters):
    """Checks whether given date is last day of that month or not, returns true if yes
        parameters:{
            "input_date":{"source":"input_config","table":"ocr","column":"Stock Date"
        }
        NOTE: works same for all months except march, if march returns true for any date
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_data(parameters)
    try:
        input_date = input_date.replace('.','-')
        input_date = input_date.replace('suspicious','')
        input_date = input_date.replace('/','-')
        input_date = input_date.replace(' ','-')
        input_date = input_date.replace('st','')
        input_date = input_date.replace('th','')
    except:
        input_date = input_date
        
    logging.info(f"date got is {input_date}")
    list_31 = ['jan','may','jul','aug','oct','dec','01','05','07','08','10','12','january','may','july','august','october','december','1','3','5','7','8']
    list_30 = ['apr','jun','sep','nov','04','06','09','11','april','june','september','november','4','6','9']
    try:
        input_list = input_date.split("-")
        if len(input_list) == 2:
            if input_list[0].lower() in list_31:
                input_date = "31-"+input_date
            elif input_list[0].lower() in list_30:
                input_date = "30-"+input_date
            elif(input_list[0].lower()=='march') or (input_list[0].lower()=='mar') or (input_list[0]=='03'):
                logging.info(input_date)
                return True
            else:
                feb_last = calendar.monthrange(int(input_list[1]),2)[1]
                input_date = str(feb_last)+"-"+input_date
        logging.info(f"Converted date is {input_date}")
    except Exception:
        input_date = input_date

    try:
        input_date = parser.parse(input_date,default=datetime(2019, 10, 3))
        date_list = str(input_date).split("-")
        logging.info(f"Date list is : {date_list}")
        input_day = date_list[2][0:2]
        input_month = date_list[1]
        if (input_month.lower()=='march') or (input_month.lower()=='mar') or (input_month=='03'):
            return True
        input_year = date_list[0]
        logging.info(f"input date is: {input_day}")
        month_last = calendar.monthrange(int(input_year),int(input_month))[1]
        logging.info(f"last day of given month is {month_last}")
        if str(input_day) == str(month_last):
            logging.info("given date and month's last date are same")
            return True
        else:
            logging.info("given date and month's last date are different")
            return False
    except Exception as e:
        logging.error("Cannot compare two dates")
        logging.error(e)
        return False


@register_method
def do_split(self,parameters):
    """ Replaces value in a string and returns the updated value
    'parameters':{
        'data':{'source':'input','value':''},
        'symbol_to_split':{'source':'input','value':''},
        'required_index':{'source':'input','value':''}
    }
    """
    logging.info(f"parameters got are {parameters}")
    symbol_to_split = parameters.get('symbol_to_split',"")
    required_index = parameters.get('required_index',"")
    data = parameters['data']
    try:
        data_split = str(data).split(symbol_to_split)
        logging.info(f"splited data is {data_split}")
        return data_split[int(required_index)]
    except Exception as e:
        logging.error("Spliting value failed")
        logging.error(e)
        return data


   

@register_method
def do_alpha_num_check(self, parameters):
    """ Returns true value if the string is Alpha or num or alnum
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
         {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    option = parameters['option']
    try:
        if option == 'alpha':
            bool_value = word.isalpha()
            logging.info(f'{word} is alpha {bool_value}')
        if option == 'numeric':
            bool_value = word.isnumeric()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'alnum':
            bool_value = word.isalnum()
            logging.info(f'{word} is numeric {bool_value}')
        return bool_value
    except Exception as e:
        logging.error("Error In do_alpha_num_check function")
        logging.error(e)
        return False

@register_method
def do_regex_columns(self, parameters):
    """ Returns a value by applying given Regex pattern on value
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'table_name':"",
                        'columns':[],
                       'regex_str':"\d{6}"
        }
        }
        NOTE: can apply for more than one column at a single time
    """
    logging.info(f"parameters got are {parameters}")
    
    table_name = parameters['table_name']
    columns = parameters['columns']
    regex_str = parameters['regex_str']
    try:
        regex = re.compile(f'{regex_str}')
    except Exception as e :
        logging.error("Error In regex pattern")
    for column in columns:
        phrase = self.data_source[table_name][column]
        logging.info(f"GOT THE VAlUE FOR COLUMN {column} IS {phrase}")
        if not phrase:
            phrase = '0'
        phrase = str(phrase).replace(",","")
        try:
            matches = re.findall(regex, str(phrase))
            if matches[0]:
                logging.info(f"LIST OF MATCHES GOT ARE : {matches}")
                matches = matches[0]
            else:
                matches = 0
        except Exception as e:
            logging.debug("REGEX MATCH GOT FAILED SO DEFAULT VALUE 0 GOT ASSIGNED")
            matches = 0
        logging.debug(f"MATCHES GOT FOR {column} COLUMN IS : {matches}")
        self.data_source[table_name][column] = str(matches)
        try:
            if table_name not in self.changed_fields:
                self.changed_fields[table_name] = {}
            self.changed_fields[table_name][column] = str(matches)
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in regex function for : {column}")
            logging.error(e)
    return True

@register_method
def do_return(self, parameters):
    """Returns the mentioned value
    'parameters':{
        'value_to_return':{"source":"input_config","table":"ocr","column":"Stock Date"}
    }
    """
    logging.info(f"parameters got are {parameters}")
    try:
        value_to_return = self.get_param_value(parameters['value_to_return'])
        return value_to_return
    except Exception as e:
        logging.error("cannot get value")
        logging.error(e)
        return ""

@register_method
def do_round(self, parameters):
    """Rounds a number to the required number of decimals
    'parameters' : {
        'value': {"source":"input_config","table":"ocr","column":""},
        'round_upto': {"source":"input_config","table":"ocr","column":""
    }
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    round_upto = self.get_param_value(parameters['round_upto'])
    try:
        value = round(float(value), round_upto)
    except Exception as e:
        logging.error("ROUND FAILED SO RETURNING SAME VALUE")
        logging.error(e)
    logging.info(f"Value after round is : {value}")
    return value


@register_method
def do_date_transform(self, parameters):
    """ Takes date as input and converts it into required format
        'parameters':{
            'input_date' : {"source":"input_config","table":"ocr","column":"Stock Date"},
            'output_format' : '%d-%b-%y'
            'output_type': {"source":"input","value":"object"}//can be object or string
        }
    """
    logging.info(f"parameters got are {parameters}")
    input_date = parameters['input_date']
    output_format = parameters['output_format']
    if input_date != "":
        try:
            output_type = self.get_param_value(parameters['output_type'])
        except Exception:
            output_type = parameters['output_type']

        date_series = pd.Series(input_date)
        try:
            if output_type == "object":
                converted_date = pd.to_datetime(date_series,dayfirst=True,errors='coerce').dt.strftime(output_format)
                logging.debug(f"######converted date: {converted_date[0]}")
                return converted_date[0]
            else:
                converted_date = pd.to_datetime(date_series,dayfirst=True,errors='coerce').dt.strftime(output_format)
                if np.isnan(converted_date[0]):
                    converted_date = input_date
                else:
                    converted_date = converted_date[0]
                return converted_date
        except Exception as e:
            logging.error("cannot convert date to the given format")
            logging.error(e)
            return input_date
    else:
        logging.info("Input_date is empty")
        return input_date

@register_method
def do_partial_match(self,parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"}
    }

    """
    logging.info(f"parameters got are {parameters}")
   
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = parameters['match_word']
    match_percent = parameters['match_percent']
    match_percent = int(match_percent)
    
    data = self.data_source.get(words_table)
    logging.info(f"data from the data_source[words_table] is {data}")
    data = pd.DataFrame(data)
    words = list(data[words_column])
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            if match_word is not None and word is not None:
                ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
                if ratio >=match_percent and ratio > max_ratio:
                    max_ratio = ratio
                    match_got = word
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)
 
    logging.info(f"match is {match_got} and ratio is {max_ratio}")
    logging.info(f"{type(max_ratio)}")
   
    return match_got


@register_method
def do_alnum_num_alpha(self,parameters):
    """ Returns true value if the string is Alpha or num or alnum
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
         {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    option = parameters['option']
    try:
        if option == 'alpha':
            bool_value = word.isalpha()
            logging.info(f'{word} is alpha {bool_value}')
        if option == 'numeric':
            bool_value = word.isnumeric()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'alnum':
            bool_value = word.isalnum()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'is_numeric':
            try:
                bool_value = float(word).is_integer()
                logging.info(f'{word} is numeric {bool_value}')
            except:
                return False
        return bool_value
    except Exception:
        return False

@register_method
def doRegex(self, parameters):

    """ Returns a value by doing Regex on given value.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    """
    logging.info(f"parameters got are {parameters}")
    phrase = parameters['phrase']
    regex_str = parameters['regex_str']
    reg_model = parameters['reg_model']

    try:
        if len(str(phrase)) <= 1:
            logging.info('phrase is empty')
            return phrase

        phrase = phrase.lower()
        logging.info(f'regex is : {regex_str}')
        logging.info(f'phrase is : {phrase}')
        regex = re.compile(f'{regex_str}')
        matches = ''

        if reg_model == 'search':
            result = re.search(regex, phrase)
            matches = result[0] if result else ''
        elif reg_model == 'match':
            matches = re.match(regex, phrase)
        elif reg_model == 'sub':
            matches = re.sub(regex, '', phrase)
        elif reg_model == 'findall':
            result = re.findall(regex, phrase)
            matches = result[0] if result else ''
        elif reg_model == 'matchall':
            result = re.findall(regex, phrase)
            matches = ''.join(result) if result else ''

        logging.info(f"Match got is {matches}")
        return matches

    except Exception as e:
        logging.error("Error In do_regex function")
        logging.error(e)
        return phrase



@register_method
def do_get_date_time(self, parameters):
    """ Returns present date and time
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'GetDateTime',
        'parameters' :{
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    try:
        date_time = datetime.now()
        logging.info(f"Got dateand time are : {date_time}")
    except Exception:
        logging.error("date time function failed")
        date_time = ""
    return date_time

@register_method
def do_date_increment(self, parameters):
    """Returns date by adding given number of days to the given input
    'parameters': {
        'input_date' : {'source':'input_config', 'table':'ocr', 'column': 'End_date'},
        'input_format' :'%d-%m-%Y',
        'increment_days': {'source':'input_config', 'table':'ocr', 'column': 'End_date'}
    }
    """
    logging.info(f"parameters got to do_date_increment are : {parameters}")
    input_date = self.get_param_value(parameters['input_date'])
    input_format = parameters['input_format']
    increment_days = self.get_param_value(parameters['increment_days'])
    increment_days = increment_days.lower().replace("days","")
    increment_days = increment_days.replace(" ","")
    try:
        converted_date = datetime.strptime(str(input_date),input_format).date()
        converted_date = converted_date + timedelta(days = int(increment_days))
        converted_date = str(converted_date)
        logging.info(f"Converted date is : {converted_date}")
        return converted_date
    except Exception as e :
        logging.error("Date incrementation failed")
        logging.error(e)
        return input_date
    

@register_method
def do_nt_path_base(self, parameters):
    """Returns base word of given path
    'parameters': {
        'input_value':{'source':'input_config', 'table':'ocr', 'column': 'End_date'}
        }
    """
    logging.info(f"parameters got for do_nt_path_base are : {parameters}")
    input_value = self.get_param_value(parameters['input_value'])
    try:
        extracted_base = ntpath.basename(str(input_value))
        logging.info(f"Extracted value from path is : {extracted_base}")
        return extracted_base
    except Exception as e:
        logging.error("Failed in extracting base name of the path")
        logging.error(e)
        return ""
    

@register_method
def amount_compare(self,parameters):
    left_param, operator, right_param = parameters['left_param'], parameters['operator'], parameters['right_param'] 
    left_param_value, right_param_value = self.get_param_value(left_param), self.get_param_value(right_param)
    logging.debug(f"left param value is {left_param_value} and type is {type(left_param_value)}")
    logging.debug(f"right param value is {right_param_value} and type is {type(right_param_value)}")
    logging.debug(f"operator is {operator}")
    try:
        left_param_value = str(left_param_value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        right_param_value = str(right_param_value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        if operator == ">=":
            logging.info(float(left_param_value) >= float(right_param_value))
            return (float(left_param_value) >= float(right_param_value))
        if operator == "<=":
            logging.info(float(left_param_value) <= float(right_param_value))
            return (float(left_param_value) <= float(right_param_value))
        if operator == ">":
            logging.info(float(left_param_value) > float(right_param_value))
            return (float(left_param_value) > float(right_param_value))
        if operator == "<":
            logging.info(float(left_param_value) < float(right_param_value))
            return (float(left_param_value) < float(right_param_value))
        if operator == "==":
            logging.info(float(left_param_value) == float(right_param_value))
            return (float(left_param_value) == float(right_param_value))
    except Exception as e:
        logging.debug(f"error in compare key value {left_param_value} {operator} {right_param_value}")
        logging.debug(str(e))
        return False
    

@register_method
def do_check_date(self,parameters):
    """
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        'parameters': {
            'value':{'source':'input_config', 'table':'ocr', 'column': 'End_date'},
            'input_format' :'%d-%m-%Y'
            }
    """
    logging.info(f"parameters got for do_check_date are {parameters}")
    value = self.get_param_value(parameters['value'])
    input_format = parameters['input_format']
    if value == '':
        return False
    elif datetime.today() > datetime.strptime(value,input_format): # %H:%M:%S
        return True
    return False

        
@register_method
def do_partial_compare(self, parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"}
    }

    """
    logging.info(f"parameters got are {parameters}")
    match_word = self.get_param_value(parameters['match_word'])
    word = self.get_param_value(parameters['word'])
    logging.info(match_word)
    logging.info(word)
    max_ratio = 0
    match_got = ""
    try:
        ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
        if ratio > 75 and ratio > max_ratio:
            max_ratio = ratio
            match_got = word
            logging.info(match_got)
            logging.info(f"match is {match_got} and ratio is {max_ratio}")
            return True
        else:
            return False
    except Exception as e:
        logging.error("cannnot find match")
        logging.error(e)

    logging.info(f"match is {match_got} and ratio is {max_ratio}")
    return match_got


@register_method
def do_partial_comparison(self, parameters):
    """ Returns True or False Based on the matching ratio
    'parameters':{
        match_word = 'helloworld'
        word = 'hello'
    }

    """
    logging.info(f"parameters got are {parameters}")
    match_word = parameters['match_word']
    word = parameters['word']
    logging.info(match_word)
    logging.info(word)
    max_ratio = 0
    try:
        ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
        if ratio > 75 and ratio > max_ratio:
            max_ratio = ratio
            logging.info(f"Ratio is {max_ratio}")
            return True
        else:
            return False
    except Exception as e:
        logging.error("cannnot find match")
        logging.error(e)

    logging.info(f"Ratio is {max_ratio}")


@register_method
def do_date_parser(self, parameters):
    """Accepts any type of input formate of date and returns required standard format

        'parameters' :{
            'input_date' : {'source':'input','value':''},
            'standard_format' : {'source':'input','value':''}
    }
    """
    logging.info(f"Parameters got in Date Parser function is {parameters}")
    standard_format = self.get_param_value(parameters['standard_format'])
    input_date = self.get_param_value(parameters['input_date'])

    try:
        parsed_date = parse(str(input_date), fuzzy=True, dayfirst=True).strftime(standard_format)
        logging.info(f"Date got after parsing is :{parsed_date}")
        return parsed_date
    except Exception as e:
        logging.error("DATE CONVERSION FAILED")
        logging.error(e)
        return input_date



@register_method
def do_amount_syntax(self, parameters):
    """Returns the amounts with .00 and with commas of the parameter value."""
    logging.info(f"parameters got are {parameters}")
    output_amount = self.get_data(parameters)
    logging.info(len(str(output_amount)))
    if len(str(output_amount)) >= 1:
        logging.info(len(str(output_amount)))
        try:
            output_amount2 = str(output_amount).replace(',','')
            output_amount=format_decimal(output_amount2, locale='en_IN')
            if '.' in output_amount:
                float_value = round(float(output_amount.replace(',','')),2)
                float_value = str(float_value)
                if float_value[-2] == '.':
                    logging.info('here')
                    output_amount = output_amount.split('.')[0]+'.'+float_value[-1:]+'0'
                else:
                    output_amount = output_amount.split('.')[0]+'.'+float_value[-2:]
            else:
                output_amount = output_amount+'.00'
            return output_amount    
        except Exception as e:
            logging.error(e)
            return output_amount
    else:
        return output_amount


@register_method
def do_date_compare(self, parameters):
    date_import=parameters['date_import']
    specific_date=parameters['specific_date']
    logging.info(f'specific date is {specific_date}')
    logging.info(f'date_import is {date_import}')
    try:
        logging.info(f'specific_date type is {type(specific_date)}')
        logging.info(f'date_import type is {type(date_import)}')
        delta = specific_date - date_import
        logging.info(f'Difference is {delta.days}')
        if delta.days >= 30:
            return "0"
        else:
            return "1"
    except Exception as e:
        logging.error("some error in the Date Compare function")
        logging.error(e)
        return "1"      



'''
@register_method
def do_date_compare(self, parameters):
    """Returns the boolean value by comparing dates.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       "parameters": {
                    "specific_date": {"source": "input_config","table": "ocr","column": "doc_date"},
                    "Operator1": "+",
                    "Operator2": ">",
                    "Days": "30",
                    "date_format": "%d-%m-%Y"
                  }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        logging.info(f"parameters got are {parameters}")
        specific_date=self.get_data(parameters['specific_date'])
        date_import=self.get_data(parameters['date_import'])
        Operator1=parameters["Operator1"]
        Operator2=parameters["Operator2"]
        Days=parameters["Days"]
        date_format=parameters["date_format"]
        def get_truth(inp, relate, cut):
            ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                '+':operator.add,
                "-":operator.sub}
            return ops[relate](inp, cut) 
        
        specific_date=datetime.strptime(specific_date,date_format)
        date_import=datetime.strptime(date_import,date_format)
        result=get_truth(date_import ,Operator1,timedelta(int(Days)))
        result=datetime.strftime(result,date_format)
        logging.info(f"result is {result}")
        fin_result=datetime.strptime(result,date_format)
        logging.info(f"The Final result is {result}")
        return get_truth(specific_date ,Operator2,fin_result)
    except Exception as e:
        logging.error(f"some error in the Date Compare function")
        logging.error(e)
        return False
'''



@register_method
def do_table_error_messages(self, parameters):
    """
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        'parameters': {
            'error_message':{'column1':['message'], 'column2':['message']},
            'input_fields' :[{"field":"Table"}]
            }
    """
    logging.info(f"parameters got for FailureMessages are {parameters}")
    error_message = parameters['error_message']
    input_fields = parameters['input_fields']
    description  = parameters['description']
    color = parameters['color']
    try:
        rule_data_db = DB('business_rules', tenant_id=self.tenant_id, **db_config)
        query_rule_data = f"select `validation_params` from `rule_data` where `case_id` = '{self.case_id}'"
        validation_params_ = rule_data_db.execute_(query_rule_data)
        validation_params = validation_params_['validation_params'][0]
        logging.info(f'type of validation params is {type(validation_params)}')
        try:
            if validation_params == "{}":
                case_id = 0
                validation_params = {}
                logging.info(f'validation params in if is {validation_params}')
            else:
                logging.info(f'validation params in else is {validation_params}')
                validation_params = json.loads(validation_params)
                case_id = int(list(validation_params.keys())[-1])+1
            logging.info(f'case_id is {case_id}')
            validation_params[case_id] = {}
            validation_params[case_id]["description"] = description
            validation_params[case_id]["output"] = ""
            validation_params[case_id]["error_message"] = error_message
            validation_params[case_id]["color"] = color
            validation_params[case_id]["input"] = []
            validation_params[case_id]["input"].insert(0,{})
            validation_params[case_id]["input"][0]["field"] = input_fields
        except Exception as e:
            logging.error("assigning of validation params failed")
            logging.exception(e)
            validation_params = {}
        logging.info(f'Final validation params is {validation_params}')
        rule_data_db = DB('business_rules', tenant_id=self.tenant_id, **db_config)
        query_rule_data = f"UPDATE `rule_data` set `validation_params` = '{json.dumps(validation_params)}' where `case_id` = '{self.case_id}'"
        rule_data_db.execute(query_rule_data)
        return True
    except Exception as e:
        logging.error("Error in Tableerrormessages Function")
        logging.exception(e)
        return False


@register_method
def do_date_conversion(self,parameters):
   
    input_date =  self.get_param_value(parameters['input_date'])
    date_val=input_date[0:2]
    logging.info(f'date_val is {date_val}')
    try:
        date_val=int(date_val)
        if date_val>12:
            from_format = ["yyyy-mm-dd",'dd-mm-yyyy',"dd/mm/yyyy","dd mm yyyy","dd,mmmm,yyyy","dd,mmm,yyyy","dd-mmmm-yyyy","dd-mmm-yyyy","dd,mmmm yyyy","dd mmm yyyy","dd/mmm/yyyy","dd mmmm yyyy","dd-Mmm-yy"]
        else:
            from_format = ['mm-dd-yyyy',"mm.dd.yyyy","mm/dd/yyyy","mm/dd/yy","mm.dd.yy","mm-dd-yy","dd,mmmm,yyyy","dd,mmm,yyyy","mmmm dd, yyyy","dd-mmmm-yyyy","dd-mmm-yyyy","yyyy-mm-dd","dd,mmmm yyyy","mmmm dd,yyyy","mmm dd,yyyy","dd mmm yyyy","dd/mmm/yyyy","mmm dd, yyyy","m/dd/yy","m/d/yy","dd-Mmm-yy","dd mmmm yyyy"]
    except Exception as e:
            from_format = ['mm-dd-yyyy',"mm.dd.yyyy","mm/dd/yyyy","mm/dd/yy","mm.dd.yy","mm-dd-yy","mmmm dd, yyyy","dd,mmmm,yyyy","dd,mmm,yyyy","dd-mmmm-yyyy","dd-mmm-yyyy","yyyy-mm-dd","dd,mmmm yyyy","mmmm dd,yyyy","mmm dd,yyyy","dd mmm yyyy","dd/mmm/yyyy","mmm dd, yyyy","m/dd/yy","m/d/yy","dd-Mmm-yy","dd mmmm yyyy"]
            logging.info('in except')
            logging.info(e)
    to_format='dd-mm-yyyy'

    try:
        
        to_format = get_output_date_format(to_format.lower())
    except Exception as e:
        logging.error("Date conversion of is failed")
        logging.error(e)
    try:
        for from_format in from_format:

            date_first_formats = {"dd mm yyyy":"%d %m %Y","dd,mmm,yyyy": "%d,%b,%Y","dd,mmmm,yyyy": "%d,%B,%Y","dd,mmmm yyyy": "%d,%B %Y","dd-mmmm-yyyy":"%d-%B-%Y","dd mmmm yyyy":"%d %B %Y","dd-mm-yy":"%d-%m-%y","dd-mm-yy hh:mm:ss":"%d-%m-%y %H:%M:%S","dd-mm-yyyy hh:mm:ss":"%d-%m-%Y %H:%M:%S","dd-mm-yyyy hh:mm":"%d-%m-%Y %H:%M","dd-bbb-yyyy":"%d-%b-%Y","dd-bbb-yy":"%d-%b-%y","dd/mm/yy hh:mm:ss":"%d/%m/%y %H:%M:%S","dd-yy-mm":"%d-%y-%m","dd mm yyyy hh:mm:ss":"%d %m %Y %H:%M:%S",
                                    "dd-mmm-yy":"%d-%b-%y","dd-yy-mmm":"%d-%y-%b","dd-mmm-yyyy":"%d-%b-%Y","dd-Mmm-yyyy":"%d-%b-%Y","dd-yyyy-mm":"%d-%Y-%m","dd-yyyy-mmm":"%d-%Y-%b","dd/mm/yy":"%d/%m/%y","dd/yy/mm":"%d/%y/%m","dd/mm/yyyy":"%d/%m/%Y","dd/mmm/yy":"%d/%b/%y","dd/yy/mmm":"%d/%y/%b","dd/mmm/yyyy":"%d/%b/%Y",
                                    "dd/yyyy/mm":"%d/%Y/%m","dd/yyyy/mmm":"%d/%Y/%b","dd-mm-yyyy":"%d-%m-%Y","dd.mm.yyyy hh:mm:ss":"%d.%m.%Y %H:%M:%S","dd.mm.yyyy":"%d.%m.%Y","dd-mm-yyyy hh:mm":"%d-%m-%Y %H:%M","dd-Mmm-yyyy, hh:mm":"%d-%b-%Y, %H:%M","dd-Mmm-yy":"%d-%b-%y","dd mmm yyyy":"%d %b %Y","dd mmm yy":"%d %b %y","dd bbb yyyy":"%d %b %Y","dd bbb yy":"%d %b %Y","dd/mm/yyyy hh:mm:ss AM/PM":"%d/%m/%Y %H:%M:%S %p",
                                    "dd/mm/yyyy hh:mm:ss am/pm":"%d/%m/%Y %H:%M:%S %p","dd/mm/yyyy hh:mm":"%d/%m/%Y %H:%M","dd/mm/yyyy hh:mm:ss":"%d/%m/%Y %H:%M:%S","dd.mm.yyyy":"%d.%m.%Y","ddmmyyyy":"%d%m%Y","ddmmyy":"%d%m%y","dmmyyyy":"%d%m%Y","dd.mm.yy":"%d.%m.%y","dd-mmm-yy hh:mm am/pm":"%d-%b-%y %H:%M %p","dd-mm-yyyy hh:mm:ss am/pm":"%d-%m-%Y %H:%M:%S %p","DD-MM-YYYY HH:MM:SS AM/PM":"%d-%m-%Y %H:%M:%S %p","DD-MM-YYYY":"%d-%m-%Y",
                                    "dd-mmm-yy hh:mm:ss":"%d-%b-%y %H:%M:%S","dd-mmm-yyyy hh:mm:ss":"%d-%b-%Y %H:%M:%S"}
            
            date_last_formats = {"mmm dd,yyyy":"%b %d,%Y","mmmm dd,yyyy":"%B %d,%Y","mmmm dd, yyyy":"%B %d, %Y","mm.dd.yy": "%m.%d.%y","mm.dd.yyyy": "%m.%d.%Y","m/dd/yy": "%m/%d/%y","m/d/yy":"%m/%d/%y","mmmm-dd-yyyy":"%B-%d-%Y","mmmm dd yyyy":"%B %d %Y","mm-yy-dd":"%m-%y-%d","bbb dd, yyyy":"%b %d %Y","mmm dd, yyyy":"%b %d, %Y","mmm dd, yyyy hh:mmam/pm":"%b %d, %Y %H:%M%p","mmm dd, yyyy h:mam/pm":"%b %d, %Y %H:%M%p","yy-dd-mm":"%y-%d-%m","yy-mm-dd":"%y-%m-%d","mm-dd-yy":"%m-%d-%y","mm-yyyy-dd":"%m-%Y-%d","mm-dd-yyyy":"%m-%d-%Y","yyyy-dd-mm":"%Y-%d-%m","yyyy-mm-dd":"%Y-%m-%d","mmm-yy-dd":"%b-%y-%d",
                                    "mmm-dd-yy":"%b-%d-%y","yy-dd-mmm":"%y-%d-%b","yy-mmm-d":"%y-%b-%-d","mmm-yyyy-dd":"%b-%Y-%d","mmm-dd-yyyy":"%b-%d-%Y","yyyy-dd-mmm":"%Y-%d-%b","yyyy-mmm-dd":"%Y-%b-%d","mm/yy/dd":"%m/%y/%d","yy/dd/mm":"%y/%d/%m",
                                    "yy/mm/dd":"%y/%m/%d","mm/dd/yy":"%m/%d/%y","mm/yyyy/dd":"%m/%Y/%d","mm/dd/yyyy":"%m/%d/%Y","yyyy/dd/mm":"%Y/%d/%m","yyyy/mm/dd":"%Y/%m/%d","mmm/yy/dd":"%b/%y/%d","mmm/dd/yy":"%b/%d/%y","yy/dd/mmm":"%y/%d/%b","yy/mmm/dd":"%y/%b/%d",
                                    "mmm/yyyy/dd":"%b/%Y/%d","mmm/dd/yyyy":"%b/%d/%Y","yyyy/dd/mmm":"%Y/%d/%b","yyyy/mmm/dd":"%Y/%b/%d","yyyy-mm-dd hh:mm:ss+5:30":"%Y-%m-%d %H:%M:%S%z","yyyy-mm-dd hh:mm:ss":"%Y-%m-%d %H:%M:%S","yyyy:mm:dd":"%Y:%m:%d","yyyymmdd":"%Y%m%d",
                                    "mm/dd/yyyy hh:mm:ss AM/PM":"%m/%d/%Y %H:%M:%S %p","mm/dd/yyyy hh:mm:ss am/pm":"%m/%d/%Y %H:%M:%S %p","mm/dd/yyyy hh:mm:ss":"%m/%d/%Y %H:%M:%S","mm/dd/yyyy hh:mm":"%m/%d/%Y %H:%M","mm/dd/yyyyhh:mm":"%m/%d/%Y%H:%M","yyyymmddhhmmss":"%Y%m%d%H%M%S",
                                    "yyyy-mm-dd hh:mm:ss +0530":"%Y-%m-%d %H:%M:%S %z","yymmdd":"%y%m%d","yyyy-mm-dd hh:mm:ss +HHMM":"%Y-%m-%d %H:%M:%S %z","Mmm dd yyyy hh:mm AM/PM":"%b %d %Y %H:%M %p","Mmm dd yyyy hh:mmAM/PM":"%b %d %Y %H:%M%p","mmm dd yyyy hh:mm am/pm":"%b %d %Y %H:%M %p", "yyyy-mm-dd hh:mm:ss+H:MM":"%Y-%m-%d %H:%M:%S%z", "mmm dd yyyy hh:mmam/pm":"%b %d %Y %H:%M%p", "yyyy-mm-ddthh:mm:ss.000z":"%Y-%m-%dT%H:%M:%S.000Z", "mm-dd-yyyy hh:mm:ss am/pm":"%m-%d-%Y %H:%M:%S %p"}   
            def convert_date_(text, from_format_, dayfirst=False):
                try:
                    # Check if target column already has converted date from previous formats
                    try:
                        dtime = pd.to_datetime(str(text).strip(), exact=False, dayfirst=dayfirst, errors='coerce', format=from_format_)
                    except Exception:
                        dtime = pd.to_datetime(str(text).strip(), exact=False, dayfirst=dayfirst, errors='coerce', utc=True, format=from_format_)

                    dtime_str = dtime.strftime(to_format)
                    return dtime_str
                except Exception:
                    logging.error(f'date is {text}')
                    return text
            try:
                
                if from_format.lower() in date_first_formats.keys():
                    final_format = date_first_formats[from_format.lower()]
                    conv_date = convert_date_(input_date, final_format, dayfirst=True)
                elif from_format.lower() in date_last_formats.keys():
                    final_format = date_last_formats[from_format.lower()]
                    conv_date = convert_date_(input_date, final_format)
                else:
                    logging.info('in pass')
                    return input_date
            except Exception as e:
                logging.error(f"Date conversion of {input_date} is failed")
                logging.error('*@@@*'*50)
            if conv_date != input_date:
                logging.info('final date is 1')
                return conv_date
            else:
                continue
        logging.info('final date is 2')
    except Exception as e:
        logging.error("Date conversion in for loop failed")
        logging.error(e)
        return input_date
    return conv_date


def regex_fields(pharse,regex_str):
    try:
        matches= re.sub(regex_str,"", pharse)
    except Exception:
        matches = pharse
    return matches


@register_method
def doNumericExtract(self, parameters):

    """ Returns a value after extracting numeric value.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    'parameters': {'value':{'source':'input_config','table':'','column':''}
    }
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    option = parameters['option']
    
    try:
        logging.info(f"got value is : {value}")

        if option in ['Digit', 'Alnum', 'Alpha']:
            filters = {
                'Digit': lambda i: i.isdigit(),
                'Alnum': lambda i: i.isalnum(),
                'Alpha': lambda i: i.isalpha()
            }
            numeric_val = ''.join(filter(filters[option], value))
        
        elif option == 'Upper':
            numeric_val = ''.join([each.upper() if each.isalpha() else each for each in value])
        
        elif option == 'Lower':
            numeric_val = ''.join([each.lower() if each.isalpha() else each for each in value])
        
        logging.info(f"extracted value is : {numeric_val}")
        return numeric_val

    except Exception as e:
        logging.error("Error In do_numeric_extract function")
        logging.error(e)
        return value

    
@register_method
def do_transform_(self, parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    logging.info(f"parameters got are {parameters}")
    try:
        val1 = parameters['value1']
        operator = parameters['operator']
        val2 = parameters['value2']
        val1 = str(val1).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        val2 = str(val2).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        if operator == '+':
            result = float(val1)+float(val2)
        if operator == '-':
            result = float(val1)-float(val2)
        if operator == '*':
            result = float(val1)*float(val2)
        if operator == '/':
            result = float(val1)/float(val2)
        result=round(result,2)
        result=str(result)
        return result
    except Exception as e:
        logging.error("Error In do_transform_ function")
        logging.error(e)


@register_method
def do_contains_string_(self,parameters):
    """ Returns true value if the string is present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = parameters['match_word']
    
    data = self.data_source[words_table]
    data = pd.DataFrame(data)
    words = list(data[words_column])
    logging.info(f'word is:{match_word}')
    logging.info(words)
    match_got = ""
    try:
        if str(match_word).strip() != "":
            for string in words:
                if string.lower() in match_word.lower():
                    match_got = string
            logging.info(match_got)
        return match_got
    except Exception as e:
        logging.error('===> Error in do_contains_string_')
        logging.error(e)




@register_method
def do_contain_string(self, parameters):
    """
        Blockly related
        Returns true value if the sub_string is present in the main_string
    Args:
        parameters (dict): The main_string and sub_string parameter (Both are strings).
    eg:
    
                'parameters': { 'data_source': "Sector 2 Sadiq Nagar, Bangalore,Adilabad,110049,India",
                                'data':"india"
                        }
            
    """
    try:
        logging.info(f"parameters got are {parameters}")
        main_string = parameters['main_string']
        sub_string = parameters['sub_string']
        logging.info(f"data got is : {sub_string}")
        if main_string != "" and sub_string != "":
            if sub_string.strip().lower() in main_string.strip().lower():
                return True
            else :
                return False
        else:
            return False
    except Exception as e:
        logging.error('===> Error in do_contain_string')
        logging.error(e)
        return False

@register_method
def do_queue_percentage(self, parameters):
    try:
        logging.info(f"parameters got are {parameters}")
        queues = parameters['queues']
        queue = parameters['queue']
        flag = ''
        
        try:
            queues = ast.literal_eval(queues)
        except Exception:
            pass  # No need to reassign 'queues' if the evaluation fails

        for i, q in enumerate(queues):
            if isinstance(q, str) and queue == q:
                flag = i
                break  # Exit early if found
            elif isinstance(q, list) and queue in q:
                flag = i
                break  # Exit early if found
        
        if flag != '':
            try:
                percentage = (100 / len(queues)) * (flag + 1)
                percentage = round(percentage)
            except Exception:
                percentage = ''
        else:
            percentage = ''

        return percentage

    except Exception as e:
        logging.info('Error in do_queue_percentage function')
        logging.info(e)
        return ''

@register_method
def donumword_to_number_comp(self, parameters):
    logging.info(f"parameters got are {parameters}")
    word = parameters['word']
    
    if word != "":
        try:
            num_word  = w2n.word_to_num(word)
            return num_word
        except Exception as e:
            logging.info('Error in donumword_to_number_comp function')
            logging.info(e)
            return word
    else:
        return word



@register_method
def do_not_contain_string(self, parameters):
    """ Returns true value if the string is not present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            "word" : "india"
            "string_list" : ["country","australia"]
            }
    """
    logging.info(f"parameters got are {parameters}")
    word = parameters['word']
    string_list = parameters['string_list']
    if word != "":
        try:
            for string_ in string_list:
                if string_ in word:
                    return False
            return True
        except Exception as e:
            logging.info('===> Error in do_not_contain_string')
            logging.info(e)
            return False
    else:
        return False


















































@register_method
def do_type_conversion(self, parameters):
    """Returns the converted value.
    Args:
        parameters (dict): The source parameter and the datatype to be converted into. 
    eg:
        'parameters': {'value': {'source':'', 'value':''},
                        'data_type': ''
                         }
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    data_type = parameters['data_type']
    try:
        logging.info(f"Value got is : {str(value)}")
        logging.info(f"To be converted Data Type is :{data_type}")
        if value != "":
            if data_type == 'str':
                converted_value = str(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'list':
                converted_value = list(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'set':
                converted_value = set(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'tuple':
                converted_value = tuple(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'int':
                converted_value = int(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'float':
                converted_value = float(value)
                logging.info(f"The Converted value is : {converted_value}")
            else :
                logging.info(f"Data Type not found {data_type}")
            return converted_value
        else:
            return value
    except Exception as e:
        logging.error("some error in the Type Conversion function")
        logging.error(e)
        return value
    



@register_method
def to_lower(self, parameters):
    """
        Returns false if the input is empty or else returns the lower case value of given input
    Args:
        parameters (dict): For the Given input value it checks if it is empty or not and returns False or lower case value 
        Respectively.
    eg:
    
                'parameters': { 'value' : 'INPUT_Value'}
            
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    value = str(value)
    try:
        if value != "":
            logging.info(f"To be Converted value is: {value}")
            value = value.lower()
            return value
        else:
            return value
    except Exception as e:
        logging.error("Error in converting the input to lower case")
        logging.error(e)
        return value



@register_method
def do_dates_diff(self, parameters):
    """
        Returns the no. of days between the given two input dates
    Args:
        parameters (dict): For the Given input dates as dictionary, it returns the difference no of days in between
   
   eg:
             'parameters': {
                                start_date : "13-02-2023"
                                end_date : "31-08-2023"
                            }
            
    """
    logging.info(f"parameters got are {parameters}")
    start_date = parameters["start_date"]
    end_date = parameters["end_date"]
    diff = 0
    try:
        if start_date != "" and end_date != "":
            logging.info(f"start_date and end_date is : {start_date} and {end_date}")
            
            start = datetime.strptime(start_date,'%d-%m-%Y')
            end = datetime.strptime(end_date,'%d-%m-%Y')
            logging.info(f"date objects of start and end : {start},{end}")
            
            diff = end-start
            logging.info(f"Difference is {diff}")
            
            return diff.days
        else:
            return diff
    except Exception as e:
        logging.error("Error in finding the difference between the dates provided")
        logging.error(e)
        return diff
    


@register_method
def is_numeric(self,parameters):
    ''' Returns True or False Based on whether the input is numeric value or not'''
    logging.info(f"parameters got are {parameters}")
    input_val = parameters.get("input","")
    input_val = str(input_val)
    try:
        return input_val.isnumeric()
    except ValueError as e:
        logging.info(f"# error in finding whether it is numeric or not : {e}")
        return False

    
@register_method
def duplicate_check(self,parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        column_names = parameters['column_names']
        column_values = parameters['column_values']
        db_config["tenant_id"]=self.tenant_id
        case_id=self.case_id
        ocr_db = DB('extraction', **db_config)
        query = "SELECT * FROM `ocr` WHERE "
        conditions = []
        params_=[]
        
        for col,val in zip(column_names,column_values):
            conditions.append(f"{col} = %s")
            params_.append(val)
        
        conditions.append("case_id != %s")
        params_.append(case_id)
        
        query += " AND ".join(conditions)
        df = ocr_db.execute_(query, params=params_)
        
        logging.info(f"#### Constructed Query is : {query}")
        logging.info(f"#### Length of df is : {len(df)}")

        if len(df) == 0:
            return False                      
        else:
            return True
    except Exception as e:
        logging.error("Error In duplicate_check function")
        logging.error(e)
        return False
    



@register_method
def query_and_check(self,parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        column_names = parameters['column_names']
        column_values = parameters['column_values']
        table_ = parameters['table_']
        db_config["tenant_id"]=self.tenant_id
        table_db = DB('extraction', **db_config)
        query = f"SELECT * FROM {table_} WHERE "
        conditions = []
        params_=[]
        
        for col,val in zip(column_names,column_values):
            conditions.append(f"{col} = %s")
            params_.append(val)
        
        query += " AND ".join(conditions)
        df = table_db.execute_(query, params=params_)
        
        logging.info(f"#### Constructed Query is : {query}")
        logging.info(f"#### Length of df is : {len(df)}")

        if len(df) == 0:
            return False
        else:
            return True
    except Exception as e:
        logging.error("Error In query_and_check function")
        logging.error(e)
        return False
    



@register_method
def partially_compare(self, parameters):
    """ Returns True or False Based on the matching ratio
    'parameters':{
        match_word = 'helloworld'
        word = 'hello'
        match_percent = '75'

    }

    """
    logging.info(f"parameters got are {parameters}")
    match_word = parameters['match_word']
    word = parameters['word']
    match_percent = parameters['match_percent']
    logging.info(f'match_word is {match_word} its type is {type(match_word)}')
    logging.info(f'word is {word} its type is {type(word)}')
    logging.info(f'match_percent is {match_percent} its type is {type(match_percent)}')
    max_ratio = 0
    try:
        ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
        if ratio >= int(match_percent) and ratio > max_ratio:
            max_ratio = ratio
            logging.info(f"Ratio is {max_ratio}")
            return True
        else:
            return False
    except Exception as e:
        logging.info(f"Ratio is {max_ratio}")
        logging.error("Error in partially_compare function")
        logging.error(e)
        return False
    

@register_method
def do_extra_year(self, parameters):
    """ Takes date as input and converts it into the required format
    'parameters':{
        input_date : {"source":"input_config","table":"ocr","column":"Stock Date"},
        output_format : '%d-%b-%y',
        n : 1
        }
        'final_output': {"source":"input","value":"object"}  # can be object or string
    
    """
    logging.info(f"parameters got are {parameters}")
    input_date = parameters['input_date']
    output_format = parameters['output_format']
    n = parameters['n']
    n = int(n)
        
    try:
        if input_date != "":
            converted_date = pd.to_datetime(input_date, dayfirst=True, errors='coerce')
            logging.info(f"converted_date got are {converted_date}")

            years_to_add = converted_date.year + n
            logging.info(f"years_to_add got are {years_to_add}")

            final_output = converted_date.replace(year=years_to_add).strftime(output_format)
            logging.info(f"final_output got are {final_output}")
        
        else:
            logging.info("input_date is empty")
            return input_date
    
    except Exception as e:
        logging.error("cannot convert date to the given format")
        logging.info("Error in do Extra Year function")
        logging.error(e)
        return input_date



@register_method
def get_last_n_chars(self,parameters):
    """input parameters (dict): 
            {input : "Algonox Technologies" , n : "7"}

            output : 'ologies'
            """
    logging.info(f"parameters got are {parameters}")
    input_ = parameters['input']
    n = parameters['n']
    n = int(n)
    logging.info(f"input_ and n value is {input_}, {n}")
    try:
        if n < 0:
            logging.info("The n value is less than zero")
            return ""
        elif n >= len(input_):
            logging.info("The n value is grater than length of input_ string")
            return input_
        else:
            logging.info(f"The n value is {n}")
            return input_[-n:]
    except Exception as e:
        logging.info("error in 'get_last_n_chars' function")
        logging.error(e)
        return input_


@register_method
def get_next_month_first_date(self,parameters):


    logging.info(f"parameters got are {parameters}")
    input_=parameters['input']
    logging.info(f"input_  is {input_}")
    try:
        parsed_date = parser.parse(input_, dayfirst=True)
        logging.info(f"parsed date is {parsed_date}")
        next_month_date = parsed_date + relativedelta(months=1)
        logging.info(f"next month date is {next_month_date}")
        next_month_date = next_month_date.replace(day=1)
        logging.info(f"next month date1 is {next_month_date}")
        result = next_month_date.strftime("%d-%m-%Y")
        return result
    except ValueError:
        result=''
        return result

@register_method
def rb_stock_summary_table1(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    logging.info("data for extraction")
    
    try:
        logging.info("try block started")
        header_name=[{"field":"Particulars","stack_id":"sub_stack_0"},{"field":"0-5 Days","stack_id":"sub_stack_1"},{"field":"6-30 Days","stack_id":"sub_stack_2"},{"field":"31-60 Days","stack_id":"sub_stack_3"},{"field":"6-60 Days","stack_id":"sub_stack_4"},{"field":"61-90 Days","stack_id":"sub_stack_5"},{"field":"91-120 Days","stack_id":"sub_stack_6"},{"field":"121-150 Days","stack_id":"sub_stack_7"},{"field":"151-180 Days","stack_id":"sub_stack_8"},{"field":">180 Days","stack_id":"sub_stack_9"},{"field":"Total","stack_id":"sub_stack_10"}]     
        columns=[]
        logging.info(" before header for loop ")
        for i in header_name:
            columns.append(i['field'])
        logging.info("after for loop")
        table_obj = {}
        table_obj["table"] = {}
        table_obj["table"]["header"] = columns
        row_data=[{"particulars":"imported_rm","0-5_days":"rb_imported_rm_0to5","6-30_days":"rb_imported_rm_ 6to30","31-60_days":"rb_imported_rm_31to60","6-60_days":"rb_imported_rm_6to60","61-90_days":"rb_imported_rm_61to90","91-120_days":"rb_imported_rm_91to120","121-150_days":"rb_imported_rm_121to150","151-180_days":"rb_imported_rm_151to180",">180_days":"rb_imported_rm_gt180","total":""},{"particulars":"wip","0-5_days":"rb_wip_0to5","6-30_days":"rb_wip_6to30","31-60_days":"rb_wip_31to60","6-60_days":"rb_wip_6to60","61-90_days":"rb_wip_61to90","91-120_days":"rb_wip_91to120","121-150_days":"rb_wip_121to150","151-180_days":"rb_wip_151to180",">180_days":"rb_wip_gt_180","total":""},{"particulars":"fg_trading","0-5_days":"rb_fg_trading_0to5","6-30_days":"rb_fg_trading_6to30","31-60_days":"rb_fg_trading_31to60","6-60_days":"rb_fg_trading_6to60","61-90_days":"rb_fg_trading_61to90","91-120_days":"rb_fg_trading_91to120","121-150_days":"rb_fg_trading_121to150","151-180_days":"rb_fg_trading_151to180",">180_days":"rb_fg_trading_gt180","total":""},{"particulars":"vehicle_stock","0-5_days":"rb_vs_0to5","6-30_days":"rb_vs_6to30","31-60_days":"rb_vs_31to60","6-60_days":"rb_vs_6to60","61-90_days":"rb_vs_61to90","91-120_days":"rb_vs_91to120","121-150_days":"rb_vs_121to150","151-180_days":"rb_vs_151to180",">180_days":"rb_vs_gt180","total":""},{"particulars":"spares_stock","0-5_days":"rb_ss_0to5","6-30_days":"rb_ss_6to30","31-60_days":"rb_ss_31to60","6-60_days":"rb_ss_6to60","61-90_days":"rb_ss_61to90","91-120_days":"rb_ss_91to120","121-150_days":"rb_ss_121to150","151-180_days":"rb_ss_151to180",">180_days":"rb_ss_gt180","total":""},{"particulars":"consumables_packing_material","0-5_days":"","6-30_days":"","31-60_days":"","6-60_days":"","61-90_days":"","91-120_days":"","121-150_days":"","151-180_days":"",">180_days":"","total":"rb_packing_material_total"},{"particulars":"stock_in_transit","0-5_days":"","6-30_days":"","31-60_days":"","6-60_days":"","61-90_days":"","91-120_days":"","121-150_days":"","151-180_days":"",">180_days":"","total":"rb_stock_in_transit_total"},{"particulars":"total_stock","0-5_days":"","6-30_days":"","31-60_days":"","6-60_days":"","61-90_days":"","91-120_days":"","121-150_days":"","151-180_days":"",">180_days":"","total":"rb_total_stocks_total"}]
       
        query = f'SELECT * FROM `ocr` where case_id="{case_id}"'
        df = ocr_db.execute_(query)
        df.fillna('',inplace=True)
        logging.info("before row data for loop")
        for i in row_data:
            for key,value in i.items():
                try:
                    i[key] = df[value][0]
                except Exception:
                    pass
        logging.info("before table obj data dumps into table")
        table_obj["table"]["rowData"] = row_data
        logging.info(f"table_obj is {table_obj}")
        table_obj = json.dumps(table_obj)
        logging.info("after table data dumps into table")
        query1=f"UPDATE ocr SET `rb_stock_summary1`= '{table_obj}' where case_id='{case_id}'"
        
        ocr_db.execute_(query1)
        return True
    
    except Exception as e:
        logging.error("Error in rb_stock_summary_table1 function")
        logging.error(e)
        return False



@register_method
def cons_stock_table(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    logging.info("data for extraction")
    
    try:
        logging.info("try block started")
        header_name=[{"field":"Particulars","stack_id":"sub_stack_0"},{"field":"<30 Days","stack_id":"sub_stack_1"},{"field":"6-60 Days","stack_id":"sub_stack_2"},{"field":"30-90 Days","stack_id":"sub_stack_3"},{"field":"<60 Days","stack_id":"sub_stack_4"},{"field":"60-90 Days","stack_id":"sub_stack_5"},{"field":"<90 Days","stack_id":"sub_stack_6"},{"field":"90-120 Days","stack_id":"sub_stack_7"},{"field":"90-180 Days","stack_id":"sub_stack_8"},{"field":"<120 Days","stack_id":"sub_stack_9"},{"field":"120-150 Days","stack_id":"sub_stack_10"},{"field":"120-180 Days","stack_id":"sub_stack_11"},{"field":">180 Days","stack_id":"sub_stack_12"},{"field":"Unit/Quantity","stack_id":"sub_stack_13"}]
        columns=[]
        logging.info(" before header for loop ")
        for i in header_name:
            columns.append(i['field'])
        logging.info("after for loop")
        table_obj = {}
        table_obj["table"] = {}
        table_obj["table"]["header"] = columns
        row_data=[{"particulars":"co_raw_materials","<30_days":"co_rm_lt30","6-60_days":"co_rm_6to60","30-90_days":"co_rm_30to90","<60_days":"co_rm_lt60","60-90_days":"co_rm_60to90","<90_days":"co_rm_90","90-120_days":"co_rm_90to120","90-180_days":"co_rm_90to180","<120_days":"co_rm_lt120","120-150_days":"co_rm_120to150","120-180_days":"co_rm_120to180",">180_days":"co_rm_gt180","unit/quantity":"co_rm_unit_quan"},{"particulars":"co_wip","<30_days":"co__wip_lt30","6-60_days":"co_wip_6to60","30-90_days":"co_wip_30to90","<60_days":"co_wip_lt60","60-90_days":"co_wip_60to90","<90_days":"co_wip_lt90","90-120_days":"co_wip_90to120","90-180_days":"co_wip_90to180","<120_days":"co_wip_lt120","120-150_days":"co_wip_120to150","120-180_days":"co_wip_120to180",">180_days":"co_wip_gt180","unit/quantity":"co_wip_unit_quan"},{"particulars":"consumables_packing_material","<30_days":"co_con&pm_lt30","6-60_days":"co_con&pm_6to60","30-90_days":"co_con&pm_30to90","<60_days":"co_con&pm_lt60","60-90_days":"co_con&pm_60to90","<90_days":"co_con&pm_lt90","90-120_days":"co_con&pm_90to120","90-180_days":"co_con&pm_90to180","<120_days":"co_con&pm_lt120","120-150_days":"co_con&pm_120to150","120-180_days":"co_con&pm_120to180",">180_days":"co_con&pm_gt180","unit/quantity":"co_con&pm_unit_quan"},{"particulars":"co_fg","<30_days":"co_fg_lt30","6-60_days":"co_fg_6to60","30-90_days":"co_fg_30to90","<60_days":"co_fg_lt60","60-90_days":"co_fg_60to90","<90_days":"co_fg_lt90","90-120_days":"co_fg_90to120","90-180_days":"co_fg_90to180","<120_days":"co_fg_lt120","120-150_days":"co_fg_120to150","120-180_days":"co_fg_120to180",">180_days":"co_fg_gt180","unit/quantity":"co_fg_unit_quan"},{"particulars":"vehicle_stock","<30_days":"co_vs_lt30","6-60_days":"co_vs_6to60","30-90_days":"co_vs_30to90","<60_days":"co_vs_lt60","60-90_days":"co_vs_60to90","<90_days":"co_vs_lt90","90-120_days":"co_vs_90to120","90-180_days":"co_vs_90to180","<120_days":"co_vs_lt120","120-150_days":"co_vs_120to150","120-180_days":"co_vs_120to180",">180_days":"co_vs_gt180","unit/quantity":"co_vs_unit_quan"},{"particulars":"spares_stock","<30_days":"co_ss_lt30","6-60_days":"co_ss_6to60","30-90_days":"co_ss_30to90","<60_days":"co_ss_lt60","60-90_days":"co_ss_60to90","<90_days":"co_ss_lt90","90-120_days":"co_ss_90to120","90-180_days":"co_ss_90to180","<120_days":"co_ss_lt120","120-150_days":"co_ss_120to150","120-180_days":"co_ss_120to180",">180_days":"co_ss_gt180","unit/quantity":"co_ss_unit_quan"},{"particulars":"total_stock","<30_days":"co_total_stock_lt30","6-60_days":"co_total_stock_30to90","30-90_days":"co_total_stock_30to90","<60_days":"co_total_stock_lt60","60-90_days":"co_ss_60to90","<90_days":"co_ss_lt90","90-120_days":"co_total_stock_90to120","90-180_days":"co_total_stock_90to180","<120_days":"co_total_stock_lt120","120-150_days":"co_total_stock_120to150","120-180_days":"co_total_stock_120to180",">180_days":"co_total_stock_gt180","unit/quantity":"co_total_stock_unit_quan"}]
        
        query = f'SELECT * FROM `ocr` where case_id="{case_id}"'
        df = ocr_db.execute_(query)
        df.fillna('',inplace=True)
        logging.info("before row data for loop")
        for i in row_data:
            for key,value in i.items():
                try:
                    i[key] = df[value][0]
                except Exception:
                    pass
        logging.info("before table obj data dumps into table")
        table_obj["table"]["rowData"] = row_data
        logging.info(f"table_obj is {table_obj}")
        table_obj = json.dumps(table_obj)
        logging.info("after table data dumps into table")
        query1=f"UPDATE ocr SET `co_stock_table`= '{table_obj}' where case_id='{case_id}'"
        
        ocr_db.execute_(query1)
        return True
    
    except Exception as e:
        logging.error("Error in cons_stock_table function")
        logging.error(e)
        return False


@register_method
def cons_credi_table(self,parameters):
    logging.info(f"parameters got are {parameters}")
    logging.info("Consumers creditors table started")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    logging.info("data for extraction")
    
    try:
        logging.info("try block started")
        header_name=[{"field":"Particulars","stack_id":"sub_stack_0"},{"field":"Amount","stack_id":"sub_stack_12"},{"field":"No. of Creditors","stack_id":"sub_stack_13"}]        
        columns=[]
        logging.info(" before header for loop ")
        for i in header_name:
            columns.append(i['field'])
        logging.info("after for loop")
        table_obj = {}
        table_obj["table"] = {}
        table_obj["table"]["header"] = columns
        row_data=[{"particulars":"co_lcbc","amount":"co_lcbc_amount","no._of_creditors":"co_lcbc_noof_creditors"},{"particulars":"co_otherthan_lcbc","amount":"co_otherthan_lcbc_amount","no._of_creditors":"co_otherthan_lcbc_nof_cred"},{"particulars":"co_spares_creditors","amount":"co_spares_creditors_amount","no._of_creditors":"co_spares_nof_cred"},{"particulars":"co_total_creditors","amount":"co_total_creditors_amount","no._of_creditors":"co_total_credi_nof_credit"}]
        query = f'SELECT * FROM `ocr` where case_id="{case_id}"'
        df = ocr_db.execute_(query)
        df.fillna('',inplace=True)
        logging.info("before row data for loop")
        for i in row_data:
            for key,value in i.items():
                try:
                    i[key] = df[value][0]
                except Exception:
                    pass
        logging.info("before table obj data dumps into table")
        table_obj["table"]["rowData"] = row_data
        logging.info("creditors table obj")
        logging.info(f"table_obj is {table_obj}")
        table_obj = json.dumps(table_obj)
        logging.info("after table data dumps into table")
        
        query1=f"UPDATE ocr SET `co_creditors_table`= '{table_obj}' where case_id='{case_id}'"
        ocr_db.execute_(query1)
        return True
    
    except Exception as e:
        logging.error("Error in cons_credi_table function")
        logging.error(e)
        return False

@register_method
def month_and_year(self,parameters):


    logging.info(f"parameters got are {parameters}")
    input_=parameters['input']
    logging.info(f"input_  is {input_}")
    try:
        match = re.search(r'\w+\s\d{4}', input_)
        logging.info(f"match date is {match}")
        if match:
            extracted_date = match.group()
            logging.info(f"extracted date is {extracted_date}")
            result = extracted_date
            return result
            
    except ValueError:
        result=input_
        return result
        
@register_method
def do_validation_params(self, parameters):
    logging.info(f"parameters got are {parameters}")

    validation_params_ = {}
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction",**db_config)
    try:
        colour = parameters['colour']
        error_message = parameters['error_message']
        source = parameters['source']
        
        validation_params_[source] = {'color':colour, 'error_message':error_message}
        logging.info(f"validation_params: {validation_params_}")
        final_out = json.dumps(validation_params_)
        
    except Exception as e:
        logging.error("cannot add validation comments")
        logging.error(e)
        final_out = json.dumps(validation_params_)

    query1 = f"select `case_id` from rule_data where case_id='{case_id}'"
    query1_data = ocr_db.execute_(query1)

    if query1_data.empty:
        insert_data = {
                        'case_id': case_id,
                        'validation_params': validation_params_
                    }
        ocr_db.insert_dict(insert_data, 'rule_data')
    else:
        update = {
            'validation_params': validation_params_
        }
        where = {
            'case_id': case_id
        }
        ocr_db.update('process_queue', update=update, where=where)
    return final_out



@register_method
def get_month_last_date(self,parameters):
    logging.info(f"parameters got are {parameters}")
    input_=parameters['input']
    logging.info(f"input_  is {input_}")

    try:
        parsed_date = datetime.strptime(input_, "%d-%m-%Y")
        logging.info(f"parsed date is {parsed_date}")
        year = parsed_date.year
        logging.info(f"year is {year}")
        month = parsed_date.month
        logging.info(f"month is {month}")
        last_day = calendar.monthrange(year,month)[1]
        logging.info(f"last day  is {last_day}")
        result = datetime(year,month,last_day)
        logging.info(f"result is {result}")
        
        return result
    except Exception as e:
        logging.info("Error occured during get__month_last_date")
        logging.error(e)
        
        return ''



@register_method
def  get_month_agri_fifteenth(self,parameters):
    logging.info(f"parameters got are {parameters}")
    input_=parameters['input']
    logging.info(f"input_  is {input_}")
    try:
        parsed_date = parser.parse(input_)
        logging.info(f"parsed date is fifteenth {parsed_date}")
        year = parsed_date.year
        logging.info(f"year is for fifteenth {year}")
        month = parsed_date.month
        logging.info(f"month is fifteenth is {month}")
        first_day_next_month = datetime(year, month, 1) + timedelta(days=calendar.monthrange(year, month)[1])
        result = first_day_next_month.replace(day=15)
        logging.info(f"result is fifteenth is {result}")
        return result
    except Exception as e:
        logging.info("Error occured during get__month_agri_fifteenth")
        logging.error(e)
        
        return ''






@register_method
def merge_dict(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    a = parameters['a']
    b = parameters['b']
    
    merged_dict = {}
    try:
        
        l=[]
        l.append(a)
        l.append(b)
        
        for i in l:
            if i!=None and i!='NULL' and i!='null' and i!='None' and i!='none' and i!='':
                i = json.loads(i)
                merged_dict.update(i)
        merged_dict = json.dumps(merged_dict)
        return merged_dict
    except Exception as e:
        logging.error(f"Error occured in merge_dict function{e}")
        
        return {}        






@register_method
def get_data_dict(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    dic = parameters['input']
    col = parameters['col_name']
    #logging.info(f"Input Dictionary is {dic}")
    logging.info(f"Type of the Input is {type(dic)}")
    logging.info(f"Key from which we need value is {col}")
    value = '0'
    
    try:
        col = re.sub('[^a-zA-Z\d]', '', col.lower())
        logging.info(f"col_cleaned is {col}")

        if isinstance(dic, str):
            dic = json.loads(dic)

        found_key = None
        value = 0
        for key, val in dic.items():
            check_cleaned = re.sub('[^a-zA-Z\d]', '', key.lower())
            if col == check_cleaned:
                found_key = key
                print(f"found key is {found_key} and key is {key}")
                break
        if found_key is not None:
            if 'a.v' in dic[found_key]:
                value = list(dic[found_key]['a.v'].values())[0] 
                value = float(value.replace(',', ''))
                print(f"value is {value}")
            else:
                value = dic[found_key]
                value = float(value.replace(',', ''))
                print(f"value is {value}")
        else:
            print("Column not found")
            value = 0

        return float(value) if value else 0
    except Exception as e:
        logging.info("Error Occured in the get_data_dict Function")
        logging.error(e)
        return 0
    
@register_method
def dosummary(self, parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction", **db_config)
    field_changes = self.field_changes
    tables = ["STOCK STATEMENT", "DEBITORS STATEMENT", "CREDITORS"]

    table = next((table_ for table_ in field_changes if table_ in tables), None)
    if not table:
        return False

    try:
        query = f"SELECT `{table}` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df = json.loads(df[table][0])

        headers = [header for header in df[0]["header"] if header not in ['Total', 'Unit/Quantity', 'No. of debtors', 'No. of creditors']][1:]
        row_data_ = df[0]["rowData"][:-1]

        if row_data_:
            l = []
            for j in headers:
                sum_ = sum(float(i.get(j, 0)) for i in row_data_ if i.get(j))
                l.append(str(sum_))

            last_dic = df[0]["rowData"][-1]
            for i, header in enumerate(headers):
                last_dic[header] = l[i]

            row_data_.append(last_dic)
            res = {"header": df[0]["header"], "rowData": row_data_}
            c_dict = json.dumps([res])

            query1 = f"UPDATE `custom_table` SET `{table}` = %s WHERE case_id = %s"
            params1 = [c_dict, case_id]
            self.cus_table = table
            ocr_db.execute_(query1, params=params1)

            return table
        else:
            return True

    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False

    
@register_method
def dosummary_1(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    field_changes=self.field_changes
    tables=["STOCK STATEMENT","DEBITORS STATEMENT","CREDITORS"]
    for table_ in field_changes:
        if table_ in tables:
            table= table_
    try:
        query =f"SELECT `{table}` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df[table][0]
        df=json.loads(df)

        header2=df[0]["header"]
        logging.info(f"Headers is for df of two {header2}")
        headers=df[0]["header"]
        logging.info(f"Headers is for df of zero {headers}")
        
        headers = [header for header in headers if header not in ['Total', 'Unit/Quantity','No. of debtors','No. of creditors']]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:]
        logging.info(f"Row data is {row_data_}")
        for x in row_data_:
            logging.info(f"x value is  {x}")
            sum_=0
            for y in headers:
                logging.info(f"y value is {y}")
                try:
                    sum_+=float(x[y])
                except Exception:
                    pass
            t=[str(sum_)]
            if 'Total' in x:
                x['Total'] =str(sum_) 
            else:
                x.update({'Total': str(sum_)}) 
            logging.info(f"T value is {t}")        
        res={}
        res["header"]=header2
        res["rowData"]=row_data_
        logging.info(f"res values is {res}")
        li=[]
        li.append(res)
        c_dict_=json.dumps(li)
        logging.info(f"c dictionary dumps check {c_dict_}")
        query1 = f"UPDATE `custom_table` SET `{table}` = %s WHERE case_id = %s"
        params1 = [c_dict_, case_id]
        self.cus_table=table
        ocr_db.execute_(query1, params=params1)
        return table
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False







@register_method
def dosummary_debtors(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    try:
        query = "SELECT `DEBITORS STATEMENT` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df['DEBITORS STATEMENT'][0]
        df=json.loads(df)
        
        header1=df[0]["header"]
        headers=df[0]["header"]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:-1]
        l=[]
        for j in headers:
            sum_=0
            for i in row_data_:
                try:
                    sum_+=float(i[j])
                except Exception:
                    pass
            l.append(str(sum_))
        last_dic=df[0]["rowData"][-1]
        for i in range(len(headers)):
            last_dic[headers[i]]=l[i]
        row_data_.append(last_dic)
        res={}
        res["header"]=header1
        res["rowData"]=row_data_
        li=[]
        li.append(res)
        c_dict=json.dumps(li)
        logging.info(f"c dictionary dumps check {c_dict}")
        query1 = "UPDATE `custom_table` SET `DEBITORS STATEMENT` = %s WHERE case_id = %s"
        params1 = [c_dict, case_id]
        ocr_db.execute_(query1, params=params1)
        self.cus_table_deb=True
        return True
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False






@register_method
def dosummary_creditors(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    try:
        query = "SELECT `CREDITORS` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df['CREDITORS'][0]
        df=json.loads(df)
        
        header1=df[0]["header"]
        headers=df[0]["header"]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:-1]
        l=[]
        for j in headers:
            sum_=0
            for i in row_data_:
                try:
                    sum_+=float(i[j])
                except Exception:
                    pass
            l.append(str(sum_))
        last_dic=df[0]["rowData"][-1]
        for i in range(len(headers)):
            last_dic[headers[i]]=l[i]
        row_data_.append(last_dic)
        res={}
        res["header"]=header1
        res["rowData"]=row_data_
        li=[]
        li.append(res)
        c_dict=json.dumps(li)
        logging.info(f"c dictionary dumps check {c_dict}")
        query1 = "UPDATE `custom_table` SET `CREDITORS` = %s WHERE case_id = %s"
        params1 = [c_dict, case_id]
        ocr_db.execute_(query1, params=params1)
        self.cus_table_crd=True
        return True
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False                 











@register_method
def add_columns_values(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    value = parameters['input']
    col = parameters['col_name']
    col1 = parameters['col_name1']
    logging.info(f"Input Dictionary is {value}")
    logging.info(f"Key from which we need value is {col}")
    logging.info(f"Type of the Input is {type(col)}")
    logging.info(f"Key from which we need value is {col1}")
    try:
        c = json.loads(col)
        for key in c:
            if key == col1:
                c[col1]=value
                
        logging.warning(f"Overall dictionary {c}")
        logging.warning(f"Overall dictionary {type(c)}")
        c = json.dumps(c)
        
        return c     

    except Exception as e:
        logging.info("Error Occured in the get_data_dict Function")
        logging.error(e)
        return col

@register_method
def month_in_words(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    month_number=parameters['month_number']
    logging.info(f"month_number  is {month_number}")
    try:
        month_number = int(month_number)

        if 1 <= month_number <= 12:
            month_name = calendar.month_name[month_number]
            return month_name 
        else:
            return ''

    except Exception as e:
        logging.info("invalid month number")
        logging.error(e)

@register_method
def assign_value_json(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['data']
    key_data=parameters['key_data']
    value_data=parameters['value_data']
    
    logging.info(f"key data is {key_data}")
    logging.info(f"value data is {value_data}")
    try:
        data = json.loads(data)
    
        value_data_str = str(value_data)
        if value_data_str!='0' and value_data_str!='0.0':
            data[key_data] = value_data_str
        else:
            data[key_data] = ''
        updated_json_data = json.dumps(data)
        logging.info(f"updated json data is  {updated_json_data}")
        return updated_json_data
    except Exception as e:
        logging.info("invalid json")
        logging.error(e)



@register_method
def margin_data(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['party_id']
    
    add=parameters['conv']
    logging.info(f"data is {data}")
    
    db_config['tenant_id'] = 'hdfc'
    ocr_db=DB("extraction",**db_config)
    logging.info("data for extraction")
    try:
        query = f"SELECT COMPONENT_NAME, MARGIN from MARGIN_MASTER where PARTY_ID = '{data}'"
        df = ocr_db.execute_(query)
        logging.info(f"df is  {df}")

        if not isinstance(df, bool) and not df.empty:
            logging.info(f"Again df is {df}")
            rm_margin = df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].empty else None
            logging.info(f"rm margin is {rm_margin}")
            wip_margin = df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].empty else None
            logging.info(f"wip_margin is{wip_margin}")
            fg_margin = df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].empty else None
            logging.info(f"fg margin is {fg_margin}")
            stores_and_spares_margin = df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].empty else None
            logging.info(f"stores and spares {stores_and_spares_margin}")
            if rm_margin==wip_margin and rm_margin==fg_margin and rm_margin==stores_and_spares_margin and wip_margin==fg_margin and wip_margin==stores_and_spares_margin and fg_margin==stores_and_spares_margin:
                margings = float(add) * (float(rm_margin)% 100)
                return margings
    except Exception as e:
        logging.error(f"Error occurred: {e}")








@register_method
def margin_data_different(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['party_id']
    data1=parameters['data_type_1']
    data2=parameters['data_type_2']
    data3=parameters['data_type_3']
    data4=parameters['data_type_4']
    
    
    logging.info(f"data is {data}")
    
    db_config['tenant_id'] = 'hdfc'
    ocr_db=DB("extraction",**db_config)
    logging.info("data for extraction")
    try:
        query = f"SELECT COMPONENT_NAME, MARGIN from MARGIN_MASTER where PARTY_ID = '{data}'"
        df = ocr_db.execute_(query)
        logging.info(f"df is  {df}")

        if not isinstance(df, bool) and not df.empty:
            logging.info(f"Again df is {df}")
            rm_margin = df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].empty else None
            logging.info(f"rm margin is {rm_margin}")
            wip_margin = df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].empty else None
            logging.info(f"wip_margin is{wip_margin}")
            fg_margin = df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].empty else None
            logging.info(f"fg margin is {fg_margin}")
            stores_and_spares_margin = df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].empty else None
            logging.info(f"stores and spares {stores_and_spares_margin}")
            if rm_margin!=wip_margin and rm_margin!=fg_margin and rm_margin!=stores_and_spares_margin and wip_margin!=fg_margin and wip_margin!=stores_and_spares_margin and fg_margin!=stores_and_spares_margin:
                margings = float(data1) * (float(rm_margin)% 100)
                margings_1 = float(data2) * (float(wip_margin)% 100)
                margings_2 = float(data3) * (float(fg_margin)% 100)
                margings_3= float(data4) * (float(stores_and_spares_margin)% 100)
                total=margings+margings_1+margings_2+margings_3
                return total

    except Exception as e:
        logging.error(f"Error occurred: {e}")        

    

@register_method
def add_key_value(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['data']
    key_data=parameters['key_data']
    value_data=parameters['value_data']
    
    logging.info(f"key data is {key_data}")
    logging.info(f"value data is {value_data}")
    try:
        data = json.loads(data)
    except json.JSONDecodeError:
        print("Invalid JSON data provided.")
        return None
    data[key_data] = value_data
    updated_json_data = json.dumps(data)
    logging.info(f"updated json data is  {updated_json_data}")
    return updated_json_data


















@register_method
def checking_files(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    queues_db=DB("queues",**db_config)
    try:
        query = "SELECT `file_name` FROM `process_queue` WHERE case_id = %s"
        params = [case_id]
        df = queues_db.execute_(query, params=params)
        file_name=df['file_name'][0]
        file_name = file_name.split("_")
        party_id = file_name[0]
        query2 = f"SELECT COUNT(*) as count from `process_queue` where file_name LIKE '%%{party_id}%%'"
        count = queues_db.execute_(query2)
        count = count['count'][0]
        if count>1:
            return "Yes"
        else:
            return "No"
    except Exception as e:
        logging.error(e)
        return "No"

def normalize_component_name(name):
    
    normalized_name = re.sub(r'\s+', '', name).lower()
    return normalized_name
@register_method 
def margin_for_extracted_fields(self, parameters):
    logging.info(f"Parameters got are {parameters}")
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction", **db_config)
    try:
        columns = ["STOCKS", "DEBTORS", "CREDITORS","ADVANCES"]
        for column in columns:
            query = f"SELECT `{column}` FROM `OCR` WHERE case_id = %s"
            params = [case_id]
            result = ocr_db.execute_(query, params=params).to_dict(orient='records')
            query = "SELECT `PARTY_ID` FROM `OCR` WHERE case_id = %s"
            params = [case_id]
            result_ = ocr_db.execute_(query, params=params)
            party_id = result_['PARTY_ID'][0]
            logging.info(f"result for {column}: {result}")

            if result:  
                df = result[0][column]  
                logging.info(f"df for {column}: {df}")
                data_dict = json.loads(df)
            else:
                data_dict={}
                
            key_component_map = {"Raw Materials":["RAW MATERIALS INSURED"],"Finished Goods":["FINISHED GOODS INSURED"],"Total Stock":["TOTAL STOCKS INSURED"],"Work in Process":["WORK IN PROGRESS INSURED"],"Stores and Spares":["STORES & SPARES INSURED"],"Stock in Transit":["STOCK IN TRANSIT"],"Consumable and Spares":["CONSUMABLE SPARES INSURED"],"goods in transist":["GOODS IN TRANSIT INSURED"],"Domestic Stock":["DOMESTIC STOCKS INSURED"],"Export Stock":["EXPORT STOCKS INSURED"],"Sales":["SALES"],"Total Debtors":["DEBTORS","RECEIVABLES","BOOK DEBTS"],"Debtors <30 days":["Debtors <30 days","BOOK DEBTS UPTO 30 DAYS"],"Debtors <60 days":["Debtors <60 days","BOOK DEBTS UPTO 60 DAYS"],"Debtors <90 days":["Debtors <90 days","BOOK DEBTS UPTO 90 DAYS"],"Debtors <120 days":["Debtors <120 days","BOOK DEBTS UPTO 120 DAYS"],"Debtors <150 days":["Debtors <150 days","BOOK DEBTS UPTO 150 DAYS"],"Debtors <180 days":["Debtors <180 days","BOOK DEBTS UPTO 180 DAYS","EXPORT DEBTORS<180 DAYS INSURED"],"Debtors - Exports":["BOOK DEBTS -EXPORTS"],"Debtors - Domestic":["BOOK DEBTS -DOMESTIC"],"Debtiors of Group Companies":["DEBTORS OF GROUP COMPANIES"],"Receivables":["RECEIVABLES"],"Domestic receivables":["DOMESTIC RECEIVABLES"],"Export receivables":["EXPORT RECEIVABLES"],"Total Creditors":["CREDITORS","Trade Creditors"],"Creditors PC":["CREDITORS (PC)"],"Unpaid Stocks":["UNPAID STOCKS"],"DALC":["LESS : DALC"],"Advances paid to suppliers":["ADD : ADVANCE TO SUPPLIER"],"Debtors PC":["DEBTORS (PC)"]}
            
            data_dict_ = {}
 
            for key, value in data_dict.items():
                
                if not value:
                    
                    if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
                        for row in data_dict['tab_view']['rowData']:
                            
                            if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
                                
                                row['margin'] = ""
                                row['aging'] = ""
 

            # Iterate over each key-value pair in the data dictionary
                else:
                    # Get the component name and margin from the mapping and database
                    component_names = key_component_map.get(key, [])
                    component_names = [component.strip() for component in component_names]
                    normalized_component_names = [normalize_component_name(name) for name in component_names]
                    margin_row = None
                    for component_name in normalized_component_names:
                        
                        query = f"SELECT `AGE`,`MARGIN` FROM AGE_MARGIN_WORKING_UAT WHERE  REPLACE(TRIM(UPPER(COMPONENT_NAME)), ' ', '') = '{component_name.upper()}' and PARTY_ID='{party_id}' "
                        margin_row = ocr_db.execute_(query).to_dict(orient='records')
                        logging.info(f"margin_row: {margin_row}")
                        if margin_row:
                            break
                    if margin_row and value:
                        margin = margin_row[0]['MARGIN']
                        age = margin_row[0]['AGE']
                        
                        logging.info(f"age: {age}")
                        
                        if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
                            for row in data_dict['tab_view']['rowData']:
                                logging.info(f"field_name: {row['fieldName']}")
                                logging.info(f"key is: {key}")
                                if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
                                    row['margin'] = margin
                                    row['aging'] = age
                    

                    
            final_data_dict = {**data_dict_,**data_dict}
            logging.info(f"final data dict: {final_data_dict}")
            
            
            chunk_size = 4000 
            value=json.dumps(final_data_dict)
            logging.info(f"Updated JSON data: {final_data_dict}")

            chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


            sql = f"UPDATE ocr SET {column} = "

            # Append each chunk to the SQL query
            for chunk in chunks:
                sql += "TO_CLOB('" + chunk + "') || "

            # Remove the last ' || ' and add the WHERE clause to specify the case_id
            sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
            
            ocr_db.execute_(sql)
            


    except Exception as e:
        logging.error(f"Unable to execute the python code: {e}")
@register_method
def array_data_append(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    input_column=parameters['input_column']
    output_column=parameters['output_column']
    logging.info(f'input_column = {input_column}')
    logging.info(f'output_column = {output_column}')

    try:
        input_column = json.loads(input_column)
        output_column = json.loads(output_column)if output_column is not None else []
        result = input_column + output_column if output_column is not None else input_column
        logging.info(f'result is  = {result}')
        result = json.dumps(result)
        return result
        
        

    except Exception:
            logging.error("Invalid data provided.",exc_info=True)
            return None
    




