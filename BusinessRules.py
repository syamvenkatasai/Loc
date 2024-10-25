try:
    import Lib
    from db_utils import DB
    import _StaticFunctions
    import _BooleanReturnFunctions
    import _AssignFunction
except Exception:
    from . import Lib
    from . import _StaticFunctions
    from db_utils import DB
    from . import _BooleanReturnFunctions
    from . import _AssignFunction



try:
    from ace_logger import Logging
    logging = Logging()
except Exception:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 

import os
import json
# one configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}




@Lib.add_methods_from(_StaticFunctions, _BooleanReturnFunctions, _AssignFunction) 
class BusinessRules():
    
    def __init__(self, case_id, rules, table_data, decision = False):
        self.case_id = case_id
        self.rules = rules
        self.data_source = table_data
        self.is_decision = decision
        self.validation_output = False
        self.show_validation_data = False
        self.tenant_id = ""
        self.return_vars="return_data"

        # fields which we are maintaining
        self.changed_fields = {}
        self.params_data = {}
        self.params_data['input'] = []
        self.params_data['input_validation'] = [] # validation params

        # all the tables are maintained in the tables dictionary with key as columnn name in database
        self.tables = {}

    

    def function_builder(self, method_string):
    
    

        def fun():
            try:
                

                return_list = self.return_vars.split(",")
                logging.info(f"Return List is {return_list}")
               

                globals()["BR"]=self
                
                exec(method_string,globals(),globals())
                
                return_dict = {}
                

               

                for param in return_list:
                    assign_var = globals().get(param,'')
                    logging.info(f"Assign var is {assign_var}")
                    if assign_var is None:
                        assign_var = ""
                    return_dict[param] = assign_var

                


                print(f"####### RETURN DICT {return_dict}")
                return True,return_dict
            except Exception as e:
                logging.info("###### Error in executing Python Code")
                logging.exception(e)
                return False,str(e)

        return fun

    def evaluate_business_rules(self):
        """Evaluate all the rules"""
        for rule in self.rules:
            logging.info("\n Evaluating the rule: " +f"{rule} \n")
            try:
                output = self.evaluate_rule(rule)
                logging.info("\n Output: " +f"{output} \n")
            except Exception as e:
                logging.error("Error evaluating the rule")
                logging.error(e)
        # update the changes fields in database
        logging.info(f"\nchanged fields are \n{self.changed_fields}\n")
        return self.changed_fields
    
    def evaluate_rule(self,rule):
        """Evaluate the rule"""
        logging.info(f"\nEvaluating the rule \n{rule}\n")

        exec_fun = self.function_builder(self.rules)

        try:
            _,return_data = exec_fun()

            logging.info(f"############# Return data from exe function: {return_data}")

        except Exception as e:
            logging.info("##### Error in Business Rules Evaluation")
            logging.exception(e)
            return {}

        return return_data

        



        
    
    def conditions_met(self, conditions):
        """Evaluate the conditions and give out the final decisoin
        
        """
        eval_string = ''
        # return True if there are no conditions...that means we are doing else..
        if not conditions:
            return True
        # evaluate the conditions
        for condition in conditions:
            logging.info(f"Evaluting the condition {condition}")
            if condition == 'AND' or condition == 'OR':
                eval_string += ' '+condition.lower()+' '
            else:
                eval_string += ' '+str(self.evaluate_rule(condition))+' '
        logging.info(f"\n eval string is {eval_string} \n output is {eval(eval_string)}")
        return eval(eval_string)

    def evaluate_condition(self, evaluations):
        """Execute the conditional statements.

        Args:
            evaluations(dict) 
        Returns:
            decision(boolean) If its is_decision.
            True If conditions met and it is done with executions.
            False For any other case (scenario).
        """
        for each_if_conditions in evaluations:
            conditions = each_if_conditions['conditions']
            executions = each_if_conditions['executions']
            logging.info(f'\nconditions got are \n{conditions}\n')
            logging.info(f'\nexecutions got are \n{executions}\n')
            decision = self.conditions_met(conditions)
            
            """
            Why this self.is_decision and decision ?
                In decison tree there are only one set of conditions to check
                But other condition rules might have (elif conditions which needs to be evaluate) 
            """
            if self.is_decision:
                if decision:
                    for rule in executions:
                        self.evaluate_rule(rule)
                logging.info(f"\n Decision got for the (for decision tree) condition\n {decision}")    
                return decision
            if decision:
                for rule in executions:
                    self.evaluate_rule(rule)
                return True
        return False

    def get_table_data(self, table_name, column_name):
        """Store the table data in the tables dict"""
        self.tables[column_name] = self.data_source[table_name][column_name]
    
    def evaluate_table(self, rule):
        """Evaluate the table rule on all the rows"""
        # store the required tables in the tables dict
        self.get_table_data(rule['table'],rule['column'])

       
        rows = json.loads(self.tables[rule['column_name']])
        for index in range(1, len(rows)):
            self.index = index
            self.evaluate_rule(rule['evaluate_rule'])
        
        # update the changed fields
        logging.info(f"after assigning table : {self.tables[rule['column_name']]}")
        if rule['table_name'] not in self.changed_fields:
            self.changed_fields[rule['table_name']] = {}
        self.changed_fields[rule['table_name']][rule['column_name']] = json.dumps(self.tables[rule['column_name']])

    def get_param_value(self, param_object):
        """Returns the parameter value.

        Args:
            param_object(dict) The param dict from which we will parse and get the value.
        Returns:
            The value of the parameter
        Note:
            It returns a value of type we have defined. 
            If the parameter is itself a rule then it evaluates the rule and returns the value.
        """
        logging.info(f"\nPARAM OBJECT IS {param_object}\n")
        param_source = param_object['source']
        if param_source == 'input_db':
            db = param_object['database']
            db = db.split('_')[1]
            table_key = param_object['table']
            column_key = param_object['column']
           
            db = DB(f'{db}', **db_config)
            query = f"SELECT `{column_key}` from `{table_key}` where `case_id`='{self.case_id}'"
            
            value = ""
            try:
                result_df = db.execute_(query)
                if not result_df.empty:
                    value = list(result_df[column_key])[0]
                else:
                    value = "UNIDENTIFIED VALUE"
            except Exception as e:
                logging.error("cannot execute the query and get the value for the caseid.Please check the rule")
                logging.error(e)
            return value
        if param_source == 'input_config':
            table_key = param_object['table']
            column_key = param_object['column']
            table_key = table_key.strip() # strip for extra spaces
            column_key = column_key.strip() # strip for extra spaces
            logging.debug(f"\ntable is {table_key} and column key is {column_key}\n")
            try:
                data = {}
                # update params data
                data['type'] = 'from_table'
                data['table'] = table_key
                data['column'] = column_key
                data['value'] = self.data_source[table_key][column_key]
                self.params_data['input'].append(data)
                self.params_data['input_validation'].append({'field': column_key})
                return data['value']
            except Exception as e:
                logging.error("\ntable or column key not found\n")
                logging.error(str(e))
                logging.info(f"\ntable data is {self.data_source}\n")
        if param_source == 'rule':
            param_value = param_object['value']
            return self.evaluate_rule(param_value)
        if param_source == 'input':
            param_value = param_object['value']
            
            return  param_value
        if param_source == 'table':
            table_key = param_object['table_name']
            column_key = param_object['column_name']
            row_index = self.index
            try:
                table_data = json.loads(self.tables[table_key])
            except json.JSONDecodeError:
               table_data = self.tables[table_key]
            
            columns_list = [col_data[0] for col_data in table_data[0]]
            column_index = columns_list.index(column_key)
            return table_data[row_index][column_index][0]


    def update_tables(self, updates,update_table_sources):
        """Update the values in the database"""
        logging.info(f"##### Received data to be updated: {updates}")
       
        logging.info(f"##### update table sources: {update_table_sources}")
        try:
            for table, colum_values in updates.items():
                logging.info(f"table: {table}")
                for data_base, inside_tables in update_table_sources.items():
                    logging.info(f"data_base: {data_base}")
                    logging.info(f"inside_tables: {inside_tables}")
                    if table in inside_tables:
                        for col,value in colum_values.items():
                            self.data_frame[table][col]=value
                            print(F"updating is {col} with is {value} in {table}")

            return True
        except Exception as e:
            logging.error("Cannot update the database")
            logging.error(e)
            return False
        return "UPDATED IN THE DATABASE SUCCESSFULLY"

    def get_data(self, param_object):
        """Returns the parameter value.

        Args:
            param_object(dict) The param dict from which we will parse and get the value.
        Returns:
            The value of the parameter
        Note:
            It returns a value of type we have defined. 
            If the parameter is itself a rule then it evaluates the rule and returns the value.
        """
        logging.info(f"\nPARAM OBJECT IS {param_object}\n")
        param_source = param_object['source']
        if param_source == 'input_db':
            database = param_object['database']
            database = database.split('_')[1]
            table_key = param_object['table']
            column_key = param_object['column']
            db_config['tenant_id']=self.tenant_id
            
            value = ""
            try:
              
                value=self.data_frame[table_key][column_key]
                
            except Exception as e:
                logging.error("cannot execute the query and get the value for the caseid.Please check the rule")
                logging.error(e)

            logging.info(f"###### Return Value from BR.GET_DATA: {value}")
            return value
        if param_source == 'input_config':
            table_key = param_object['table']
            column_key = param_object['column']
            table_key = table_key.strip() # strip for extra spaces
            column_key = column_key.strip() # strip for extra spaces
            logging.debug(f"\ntable is {table_key} and column key is {column_key}\n")
            try:
                data = {}
                # update params data
                data['type'] = 'from_table'
                data['table'] = table_key
                data['column'] = column_key
                data['value'] = self.data_source[table_key][column_key]
                
                return data['value']
            except Exception as e:
                logging.error("\ntable or column key not found\n")
                logging.error(str(e))
                logging.info(f"\ntable data is {self.data_source}\n")
        if param_source == 'rule':
            param_value = param_object['value']
            return self.evaluate_rule(param_value)
        if param_source == 'input':
            param_value = param_object['value']
            
            return  param_value
        if param_source == 'table':
            table_key = param_object['table']
            column_key = param_object['column']
            row_index = self.index
            try:
                table_data = json.loads(self.tables[table_key])
            except json.JSONDecodeError:
               table_data = self.tables[table_key]
            
            columns_list = [col_data[0] for col_data in table_data[0]]
            column_index = columns_list.index(column_key)
            return table_data[row_index][column_index][0]

        if param_source.lower()=='calculated':
            return globals().get(param_object['value'],"")




