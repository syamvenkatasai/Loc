# please dont change this file without talking with the Author
try:
    import Lib
except Exception:
    from . import Lib
import json


__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

try:
    from ace_logger import Logging
    logging = Logging()
except Exception:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 

import json
import os
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}


@register_method
def do_assign(self, parameters):
    """Update the parameter in the data_source of this class.
    Also update the changed fields that we are keeping track of.

    Args:
        parameters (dict): the left parameters where to assign and right one what to assign
    eg:
       'parameters': {'assign_place':{'source':'input_config', table':'ocr', 'column':invoice_no},
                       'assign_value':{'source':'input', 'value':4}
                    }
    Note:
        1) Recursive evaluations of rules can be made.
        eg:
            'parameters': {'assign_table':{'source':'input', 'value':5},
                       'assign_value':{'source':'rule', 'value':rule}
                    }
                value is itself can be a rule.
    """
    logging.debug(f"##### Assign parameters got are {parameters}")
    table_key = parameters.get('table',"")
    column_key = parameters.get('column',"")
    assign_value = self.get_data(parameters)

    try:
        logging.info(f"Updated the data source with the values {table_key} {column_key}\n ")
        self.data_source[table_key][column_key] = assign_value
    except Exception as e:
        logging.error("Couldnt update the data source with the values")
        logging.error(e)
        
    # update the changed fields
    try:
        if table_key not in self.changed_fields:
            self.changed_fields[table_key] = {}
        self.changed_fields[table_key][column_key] = assign_value
        logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        return True
    except Exception as e:
        logging.error("error in assigning and updating the fields")
        logging.error(e)
    return False


# required db_utils for stats thing.
# from db_utils import DB

@register_method
def do_assign_q(self, parameters):
    """Update the parameter in the data_source of this class.
    Also update the changed fields that we are keeping track of.
    
    
    Actually modifications are being done in the database ...as we know the queue that is being 
    getting assigned ...
    
    We have to think about the updations in the database....Design changes might be required....

    Also as of now we are keeping the assign_placd so that there are less changes in the rule_strings
    and also backward compatable....
    
    Actually its not required.

    Args:
        parameters (dict): the left parameters where to assign and right one what to assign
    eg:
       'parameters': {'assign_place':{'source':'input_config', table':'ocr', 'column':invoice_no},
                       'assign_value':{'source':'input', 'value':4}
                    }
    Note:
        1) Recursive evaluations of rules can be made.
        eg:
            'parameters': {'assign_table':{'source':'input', 'value':5},
                       'assign_value':{'source':'rule', 'value':rule}
                    }
                value is itself can be a rule.
    """
    logging.debug(f"parameters got are {parameters}")
    
    assign_value = self.get_data(parameters)

    table_key = parameters['table']
    column_key = parameters['column']
    # update the data source if the value exists
    try:
        logging.info(f"Updated the data source with the values {table_key} {column_key}\n ")
        self.data_source[table_key][column_key] = assign_value
    except Exception as e:
        logging.error("Couldnt update the data source with the values")
        logging.error(e)
        
    # update the changed fields
    try:
        if table_key not in self.changed_fields:
            self.changed_fields[table_key] = {}
        self.changed_fields[table_key][column_key] = assign_value
        logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        return True
    except Exception as e:
        logging.error("error in assigning and updating the fields")
        logging.error(e)

    return False
        
    
        
        


@register_method
def do_assign_table(self, parameters):
    """ Assigns value to column in a inner table
    "parameters":{
        'table_name':"Table",
        'column_name':"RATE",
        'assign_value':{'source':'table', 'table_name':'Table','column_name':'DUTY'}
        
    }
    """
    logging.debug(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    row_index = self.index
    assign_value = self.get_data(parameters)
    try:
        try:
            table_data = json.loads(self.tables[table_name])
        except Exception:
            table_data = self.tables[table_name]
    except Exception as e:
        logging.error("Extracting of Table Data Failed")
        logging.error(e)
        table_data = []
    try:
        columns_list = [col_data[0] for col_data in table_data[0]]
        print (columns_list)
        column_index = columns_list.index(column_name)
        table_data[row_index][column_index][0] = assign_value
        self.tables[table_name] = table_data
        return True
    except Exception as e:
        logging.error("Failed in Assining value to table data")
        logging.error(e)
        return False




