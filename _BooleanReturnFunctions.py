try:
    import Lib
except:
    from . import Lib
    
try:
    from ace_logger import Logging
    logging = Logging()
except:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 




__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

@register_method
def doCompareKeyValue(self, parameters):
    """Compares the given parameters and returns whether the conditions satisfies or not.

    Args:
        parameters (dict): The left value, operator and the right value. 
    eg:
       'parameters': {'left_param':{'source':'input', 'value':5},
                       'operator':'>',
                       'right_param':{'source':'input', 'value':4}
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    """
    left_param, operator, right_param = parameters['left_param'], parameters['operator'], parameters['right_param']
    try: 
        left_param_value, right_param_value = self.get_param_value(left_param), self.get_param_value(right_param)
    except:
        left_param_value, right_param_value = parameters['left_param'], parameters['right_param']
    logging.debug(f"left param value is {left_param_value} and type is {type(left_param_value)}")
    logging.debug(f"right param value is {right_param_value} and type is {type(right_param_value)}")
    logging.debug(f"operator is {operator}")
    try:
        left_param_value = str(left_param_value).strip()
        right_param_value = str(right_param_value).strip()
        # eval does not work for empty strings..so replace them with None
        if not left_param_value:
            left_param_value = None
        if not right_param_value:
            right_param_value = None
        
        # our own ==
        if operator == '==':
            processed_left_param_value = str(left_param_value).strip().lower()
            processed_right_param_value = str(right_param_value).strip().lower()
            logging.debug(f"\n executing {processed_left_param_value} == {processed_right_param_value}\n")
            return  processed_left_param_value == processed_right_param_value
        # our own !=
        if operator == '!=':
            processed_left_param_value = str(left_param_value).strip().lower()
            processed_right_param_value = str(right_param_value).strip().lower()
            logging.debug(f"\n executing {processed_left_param_value} != {processed_right_param_value}\n")
            return  processed_left_param_value != processed_right_param_value
        
        # extend for more types and also values...for eg..>, >=, <, <=, if eval gives errors
       
        # conversion requrired for the string types
        if type(left_param_value) == str:
            left_param_value = "'"+left_param_value+"'"
        if type(right_param_value) == str:
            right_param_value = "'"+right_param_value+"'"
            
        logging.info(f"\n evaluating the expression \n")
        logging.info(str(left_param_value) + " "+operator+ " "+ str(right_param_value) + "\n")
        logging.info(eval (str(left_param_value) + " "+operator+ " "+ str(right_param_value)) + "\n")
        return eval (str(left_param_value) + " "+operator+ " "+ str(right_param_value))
    except Exception as e:
        logging.debug(f"error in compare key value {left_param_value} {operator} {right_param_value}")
        logging.debug(str(e))
        return False


@register_method
def doAmountCompare(self,parameters):
    left_param, operator, right_param = parameters['left_param'], parameters['operator'], parameters['right_param'] 
    left_param_value, right_param_value = self.get_param_value(left_param), self.get_param_value(right_param)
    logging.debug(f"left param value is {left_param_value} and type is {type(left_param_value)}")
    logging.debug(f"right param value is {right_param_value} and type is {type(right_param_value)}")
    logging.debug(f"operator is {operator}")
    try:
        left_param_value = str(left_param_value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        right_param_value = str(right_param_value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        if operator == ">=":
            print(float(left_param_value) >= float(right_param_value))
            return (float(left_param_value) >= float(right_param_value))
        if operator == "<=":
            print(float(left_param_value) <= float(right_param_value))
            return (float(left_param_value) <= float(right_param_value))
        if operator == ">":
            print(float(left_param_value) > float(right_param_value))
            return (float(left_param_value) > float(right_param_value))
        if operator == "<":
            print(float(left_param_value) < float(right_param_value))
            return (float(left_param_value) < float(right_param_value))
        if operator == "==":
            print(float(left_param_value) == float(right_param_value))
            return (float(left_param_value) == float(right_param_value))
    except Exception as e:
        logging.debug(f"error in compare key value {left_param_value} {operator} {right_param_value}")
        logging.debug(str(e))
        return False
