"""
Author: Ashyam Zubair
Created Date: 14-02-2019
"""
import json
import pandas as pd
import traceback
import sqlalchemy
import os
import re

from MySQLdb._exceptions import OperationalError
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import create_engine
from time import time
# from encrypted_passwords_loader import encrypted_paswd_to_dict

from ace_logger import Logging



logging = Logging()
# try:
#     with open('/app/passwords.json') as f:
#         password_json = json.loads(f.read())
#         logging.info(f"passwords{password_json}")
# except:
#     logging.exception(f"json loading failed") 
db_port=os.environ.get('LOCAL_DB_PORT')   
db_service=os.environ.get('DATABASE_SERVICE')  


class DB(object):
    def __init__(self, database, host='127.0.0.1', user='root', password='', port={db_port}, tenant_id=None):
        """
        Initialization of databse object.

        Args:
            databse (str): The database to connect to.
            host (str): Host IP address. For dockerized app, it is the name of
                the service set in the compose file.
            user (str): Username of MySQL server. (default = 'root')
            password (str): Password of MySQL server. For dockerized app, the
                password is set in the compose file. (default = '')
            port (str): Port number for MySQL. For dockerized app, the port that
                is mapped in the compose file. (default = '3306')
        """
        # logging.info(f"db_service:{db_service}")
        # logging.info(f"db port_test {db_port}")
        # database_password=f'{tenant_id}_{database}'
        database_password=os.environ['LOCAL_DB_PASSWORD']
        # passwordsDict = encrypted_paswd_to_dict()
        # password_test = ''
        # logging.info(f'passwords Dict---{passwordsDict}')
        # password_test=passwordsDict[database_password]
        # logging.info(f"test password...{password_test}")

        self.HOST = host
        self.USER = user
        ## database name and password are same for the table created
        self.PASSWORD = database_password
        self.PORT = db_port
        self.DATABASE = f'{tenant_id}_{database}'
        self.DATABASE_RAW = f'{database}'

        # logging.info(f'Host: {self.HOST}')
        # logging.info(f'User: {self.USER}')
        # logging.info(f'Password: {self.PASSWORD}')
        # logging.info(f'Port: {self.PORT}')
        # logging.info(f'Database: {self.DATABASE}')

        self.connect()
    
    # def __del__(self):
    #     try:
    #         logging.info(f"############### DETROYING THE DB CONNECTION")
    #         self.engine.close()
    #         # self.db_.dispose()

    #     except Exception as e:

    #         logging.info(f'########### Failed to destroy the DB COnenctions {e}')

    def __del__(self):
        try:
            logging.info(f"############### DETROYING THE DB CONNECTION")
            #self.engine.close()
            self.db_.dispose()

        except:

            logging.info(f'########### Failed to destroy the DB')


    def close(self):
        try:
            logging.info(f"############### CLosing THE DB CONNECTION")
            #self.engine.close()
            self.db_.dispose()

        except Exception as e:

            logging.info(f'########### Failed to CLose the DB COnenctions {e}')


    def connect(self, max_retry=5):
        if hasattr(self, 'engine') and self.engine:
            self.engine.dispose()        
        retry = 1
        logging.info(os.environ)
        DIALECT = os.environ.get('DIALECT')
        SQL_DRIVER = os.environ.get('SQL_DRIVER')
        SERVICE = os.environ.get('DATABASE_SERVICE')
        # self.HOST = os.environ['HOST_IP']
        # self.PORT = os.environ['DB_PORT']

        try:
            start = time()
            logging.debug(f'Making connection to `{self.DATABASE}`...')
            # logging.debug(f'Making connection to `{self.DATABASE_RAW}`...')
            # logging.debug('Reading bridge config')
            # try:
            #     if self.DATABASE in password_json:
            #         password=password_json[self.DATABASE]
            #         logging.info(f"password{password}")
            #     else:
            #         logging.info(f"in else")
            #         password=self.DATABASE  
                      
            # config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8'
            # self.HOST = '54.165.108.71'

            ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + self.DATABASE + ':' + \
                self.PASSWORD  + '@' + self.HOST + ':' + \
                str(self.PORT) + '/?service_name=' + SERVICE
                
                # ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + self.DATABASE + ':' + \
                #     '123' + '@' + self.HOST + ':' + \
                #     str(self.PORT) + '/?service_name=' + SERVICE
                
            logging.info(ENGINE_PATH_WIN_AUTH)
            #engine = create_engine(ENGINE_PATH_WIN_AUTH)
            #self.db_ = engine.connect()
            #logging.debug(f'Engine object {self.db_}')
            engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
            self.db_ = engine
            logging.debug(f'Engine object {self.db_}')
            # self.db_ = create_engine(config, connect_args={'connect_timeout': 2}, pool_recycle=300)
            logging.info(f'Engine created for `{self.DATABASE}`')
            while retry <= max_retry:
                try:
                    self.engine = self.db_
                    logging.info(
                        f'Connection established succesfully to `{self.DATABASE}`! ({round(time() - start, 2)} secs to connect)')
                    break
                except Exception as e:
                    logging.warning(
                        f'Connection failed. Retrying... ({retry}) [{e}]')
                    retry += 1
                    self.db_.dispose()
                    
        except:
            raise Exception(
                'Something went wrong while connecting. Check trace.')

    def convert_to_oracle(self, query):
        # query = query.replace('"', "'")
        # inds = [i for i in range(len(query)) if query[i] == '`']
        # for pos, ind in enumerate(inds):
        #     if pos % 2 == 0:
        #         query = query[:ind] + '"' + query[ind+1:]
        #     else:
        #         query = query[:ind] + '"' + query[ind + 1:]
        query = query.replace("`","")

        #oracle doesnt have as ''
        if "count(*) as 'count(*)'" in query.lower():
            query = query.replace("count(*) as 'count(*)'", 'count(*) "count(*)"').replace("COUNT(*) AS 'COUNT(*)'", 'COUNT(*) "COUNT(*)"')
        if "as '" in query.lower():
            query = query.replace('AS','as')
            query_arr = query.split(' ')
            # col_index = query_arr.index('as') + 1
            # col = query_arr.get(col_index)
            # if col:
            #     query =query_arr[col_index] = '"' + col + '"'
            # query = ' '.join(query_arr)
            # query = query.replace('as', '')
            index_count = 0
            for query_item in query_arr:
                if query_item == 'as':
                    col_index = index_count + 1
                    query_arr[index_count] = ''
                    query_arr[index_count+1] = '"' + query_arr[col_index].replace("'","") + '"'
                index_count+=1
            query = ' '.join(query_arr)
        
        if "%s" in query:
            j = 0
            for i in range(len(query)):
                if query.startswith('%s', i):
                    query = query[:i] + f":{chr(97+(j%26))}" +query[(i+2):]
                    j +=1

         # Table name should be in caps
        query_arr = query.split(' ')
        table_index = -1
        if 'TABLE_NAME' in query_arr:
            table_index = query_arr.index('TABLE_NAME')+1
        elif 'table_name' in query_arr:
            table_index = query_arr.index('table_name')+1
        elif 'from' in query_arr:
            table_index = query_arr.index('from') 
        elif 'FROM' in query_arr:
            table_index = query_arr.index('FROM') 
        
        if table_index != -1 and table_index < (len(query_arr) - 1):
            
            table_index+=1
            if not '.' in query_arr[table_index]:
                query = query.replace(query_arr[table_index], f'{query_arr[table_index]}')

        #where level = 1 is wrong, it should be connect by level
        # if "where level" in query.lower() or "and level" in query.lower():
        #     query = query.replace("where level", "connect by level").replace("WHERE LEVEL", "connect by level") \
        #             .replace("and level", "connect by level").replace("AND LEVEL", "connect by level")
        
        if "limit" in query or "LIMIT" in query:
           query = query.replace("LIMIT", "limit")
           query, limiters = query.split("limit")
           if "," in limiters:
               offset, limit = limiters.split(",")
           else:
               offset = 0
               limit = limiters
           table_name = query.replace("from", "").replace("FROM","")
           
           if 'order by' not in query.lower():
               query+= f" ORDER BY id OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
           else:
               query+= f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        
        # query to change columns to uppercase
        # query = query.replace('from', 'FROM').replace('where', 'WHERE')
        # if 'FROM' in query:
        #     from_index = query.index('FROM')
        #     query_split = query.split(' ')
        #     from_split_arr = query_split.index('FROM')
        #     if '*' not in query:
        #         columns = query_split[1:from_split_arr]
        #         if ',' in columns:
        #             columns = columns.split(',')
        #         logging.info(f'############coolumns:     {columns}')
        #         columns_new = []
        #         for column in columns:
        #             if '"' not in column:
        #                 column = '"' + column.replace(' ','').replace(',','') + '"'
        #             else:
        #                 column = '' + column.replace(' ','').replace(',','') + ''

        #             columns_new.append(column)
        #         columns = ','.join(columns_new)

        #         query = query_split[0] + ' ' +  columns + ' ' + ' '.join(query_split[from_split_arr:]    )
        # if 'WHERE' in query:
        #     where_index = query.index('WHERE')
        #     query_split = query.split(' ')
        #     where_split_arr = query_split.index('WHERE')
        #     conditions = ' '.join(query_split[where_split_arr+1:])
        #     columns = []
        #     clauses = []
        #     operators = []
        #     order_by_clauses = ''
        #     logging.info(f"##### conditions : {conditions}")
        #     if 'order by' in conditions:
        #         dum = conditions.split('order by')
        #         logging.info(dum)
        #         conditions = dum[0]
        #         order_by_clauses = ' order by ' + dum[1]
        #     elif 'ORDER BY' in conditions:
        #         dum = conditions.split('ORDER BY')
        #         logging.info(dum)
        #         conditions = dum[0]
        #         order_by_clauses = ' order by ' + dum[1]
        #     logging.info(f"##### conditions now : {conditions}")

        #     conditions = re.split(r'and|AND|or|OR',conditions)
        #     logging.info(f"##### conditions after : {conditions}")
        #     conditions_new = []
        #     for con in conditions:
        #         if con == 'and' or con == 'AND':
        #             clauses.append('AND')
        #         elif con == 'or' or con == 'OR':
        #             clauses.append('OR')
        #         if '>=' in con:
        #             operators.append('>=')
        #         elif '<=' in con:
        #             operators.append('<=')
        #         elif '!=' in con:
        #             operators.append('!=')
        #         elif '=' in con:
        #             operators.append('=')
        #         elif '!' in con:
        #             operators.append('!') 
            

        #     if len(clauses)>0:
        #         clauses.append('')
            
        #     index = 0
        #     for condition in conditions:
        #         column = '"'+re.split('>|<|=|!',condition)[0].replace(' ','')+'"'
        #         rest = re.split('>|<|=|!',condition)[1:]
        #         if len(clauses)>0:
        #             conditions_new.append(column+' ' +operators[index] + ' '.join(rest) + ' '+clauses[index]+' ' +  order_by_clauses)
        #         else:
        #             conditions_new.append(column+ ' '+ operators[index]+ ' '.join(rest)+' ' + order_by_clauses)

        #     logging.info(f'############coolumns where:     {conditions_new}')
            

        #     query = ' '.join(query_split[:where_split_arr+1]    ) + ' ' + ' '.join(conditions_new)
        


        
        # show columns query is different for oracle
        if "show columns" in query.lower() or "information_schema.columns" in query.lower():
            query = f""" SELECT COlUMN_NAME as Field,   COLUMN_NAME, DATA_TYPE ,NULLABLE ,DATA_DEFAULT ,IDENTITY_COLUMN  FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{query_arr[table_index].upper().replace("'","")}' order by column_id"""


        return query

    def execute(self, query, attempt=0, index='id', **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        params = kwargs['params'] if 'params' in kwargs else None
        logging.debug(f'Query before: {query}')
        # logging.debug(f'Params: {params}')
        try:
            if attempt == 0:
                query = self.convert_to_oracle(query)
            else:
                pass
            logging.debug(f'Query after: {query}')
            logging.debug(f'Engine: {self.engine}')
            # logging.debug(kwargs)
            data = pd.read_sql_query(query, self.engine,**kwargs)
            self.engine.dispose()
            logging.info(f"Query: {query} data: {data} params: {kwargs}")
            # data = pd.read_sql(query, self.engine, index_col='id', **kwargs)

            # self.engine.dispose()
        except exc.ResourceClosedError:
            logging.warning('Query does not have any value to return.')
            return True
        except exc.IntegrityError as e:
            logging.warning(f'Integrity Error - {e}')
            return False
        except (exc.StatementError, OperationalError) as e:
            logging.warning(
                f'Creating new connection. Engine/Connection is probably None. [{e}]')
            attempt += 1
            if attempt <= 5:
                self.connect()
                logging.debug(f'Attempt #{attempt}')
                # query = query.replace('"', "`")
                return self.execute(query, attempt=attempt, **kwargs)
            else:
                logging.debug(f'Maximum attempts reached. ({5})')
                return False
        except:
            logging.exception(
                'Something went wrong executing query. Check trace.')
            return False
        finally:
            self.engine.dispose()
    
        
        if isinstance(data, pd.DataFrame):
            data_cols = list(data.columns.values)
            for data_col in data_cols:
                if data_col[-1] == '_':
                    data[data_col[:-1]] = data[data_col]

                pd.DataFrame.rename(data,columns={data_col:data_col.lower()})
                try:
                    data[data_col.upper()] = data[data_col.lower()]
                except:
                    continue

        return data.where((pd.notnull(data)), None)

    def execute_(self, query, attempt=0, **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        try:
            if attempt == 0:
                query = self.convert_to_oracle(query)
            else:
                pass
            # query = self.convert_to_oracle(query)
            data = pd.read_sql(query, self.engine, **kwargs)
            self.engine.dispose()
            logging.info(f"Query: {query} data: {data} params: {kwargs}")
        except exc.ResourceClosedError:
            return True
        except exc.IntegrityError as e:
            logging.warning(f'Integrity Error - {e}')
            return None
        except (exc.StatementError, OperationalError) as e:
            logging.warning(
                f'Creating new connection. Engine/Connection is probably None. [{e}]')
            attempt += 1
            if attempt <= 5:
                self.connect()
                logging.debug(f'Attempt #{attempt}')
                return self.execute_(query, attempt=attempt, **kwargs)
            else:
                logging.debug(f'Maximum attempts reached. ({5})')
                return False
        except:
            logging.exception(
                f'Something went wrong while connecting. Check trace.')
            params = kwargs['params'] if 'params' in kwargs else None
            return False
        finally:
            self.engine.dispose()
    
        
        if isinstance(data, pd.DataFrame):
            data_cols = list(data.columns.values)
            
            for data_col in data_cols:
                if data_col[-1] == '_':
                    data[data_col[:-1]] = data[data_col]
                pd.DataFrame.rename(data,columns={data_col:data_col.lower()})
                try:
                    data[data_col.upper()] = data[data_col.lower()]
                except:
                    continue

        return data.replace({pd.np.nan: None})

    def insert(self, data, table, **kwargs):
        """
        Write records stored in a DataFrame to a SQL database.

        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.
            kwargs: Keyword arguments for pandas to_sql function.
                See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
                to know the arguments that can be passed.

        Returns:
            (bool) True is succesfully inserted, else false.
        """
        logging.info(f'Inserting into `{table}`')

        try:
            data.to_sql(table, self.engine, **kwargs)
            # try:
            #     self.execute(f'ALTER TABLE `{table}` ADD PRIMARY KEY (`id`);')
            # except:
            #     pass
            # return True
        except:
            logging.exception('Something went wrong inserting. Check trace.')
            return False

    def insert_dict(self, data, table):
        """
        Insert dictionary into a SQL database table.

        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.

        Returns:
            (bool) True is succesfully inserted, else false.
        """
        logging.info(f'Inserting dictionary data into `{table}`...')
        logging.debug(f'Data:\n{data}')

        try:
            column_names = []
            params = []

            for column_name, value in data.items():
                # column_names.append(f'`{column_name}`')
                column_names.append(f'{column_name.replace(" ", "_")}')
                params.append(value)
                # logging.info(f"column_nameeeeeeeeeeeeeeeeeeee - {column_name}")
                # logging.info(f"Valueeeeeeeeeee - {value}")
            logging.debug(f'Column names: {column_names}')
            logging.debug(f'Params: {params}')

            columns_string = ', '.join(column_names)
            # logging.info(f"Length of paraaaaaaaaaaams - {len(params)}")
            logging.debug(f'Column names: {column_names}')
            # logging.debug(f'Params: {params}')

            columns_string = ', '.join(column_names)
            # param_placeholders = ', '.join(['%s'] * len(column_names))

            # query = f'INSERT INTO `{table}` ({columns_string}) VALUES ({param_placeholders})'
            param_placeholders = ':0'
            for i in range(1,len(column_names)):
                param_placeholders += f', :{i}'
            query = f'INSERT INTO {table} ({columns_string}) VALUES ({param_placeholders})'
            return self.execute(query, params=params)
        except:
            logging.exception('Error inserting data.')
            return False

    def update(self, table, update=None, where=None, force_update=False):
        logging.info(f'Updating table: `{table}`')
        logging.info(f'Update data: `{update}`')
        logging.info(f'Where clause data: `{where}`')
        logging.info(f'Force update flag: `{force_update}`')

        try:
            set_clause = []
            set_value_list = []
            where_clause = []
            where_value_list = []

            if where is not None and where:
                for set_column, set_value in update.items():
                    set_clause.append(f'`{set_column}`=%s')
                    set_value_list.append(set_value)
                set_clause_string = ', '.join(set_clause)
            else:
                logging.error(
                    f'Update dictionary is None/empty. Must have some update clause.')
                return False

            if where is not None and where:
                for where_column, where_value in where.items():
                    where_clause.append(f'`{where_column}`=%s')
                    where_value_list.append(where_value)
                where_clause_string = ' AND '.join(where_clause)
                query = f'UPDATE `{table}` SET {set_clause_string} WHERE {where_clause_string}'
            else:
                if force_update:
                    query = f'UPDATE `{table}` SET {set_clause_string}'
                else:
                    message = 'Where dictionary is None/empty. If you want to force update every row, pass force_update as True.'
                    logging.error(message)
                    return False

            params = set_value_list + where_value_list
            self.execute(query, params=params)
            return True
        except:
            logging.exception('Something went wrong updating. Check trace.')
            return False

    def get_column_names(self, table):
        """
        Get all column names from an SQL table.

        Args:
            table (str): Name of the table from which column names should be extracted.
            database (str): Name of the database in which the table lies. Leave
                it none if you want use database during object creation.

        Returns:
            (list) List of headers. (None if an error occurs)
        """
        try:
            logging.info(f'Getting column names of table `{table}`')
            return list(self.execute(f'SELECT * FROM `{table}`'))
        except:
            logging.exception(
                'Something went wrong getting column names. Check trace.')
            return

    def execute_default_index(self, query, **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        try:
            data = pd.read_sql(query, self.engine, **kwargs).replace({pd.np.nan: None})
            self.engine.dispose()
        except exc.ResourceClosedError:
            return True
        except:
            logging.exception(
                f'Something went wrong while executing query. Check trace.')
            params = kwargs['params'] if 'params' in kwargs else None
            return False
        finally:
            self.engine.dispose()    

        return data.where((pd.notnull(data)), None)

    def get_all(self, table, discard=None, condition=None):
        """
        Get all data from an SQL table.

        Args:
            table (str): Name of the table from which data should be extracted.
            discard (list): columns to be excluded while selecting all
        Returns:
            (DataFrame) A pandas dataframe containing the data. (None if an error
            occurs)
        """
        logging.info(f'Getting all data from `{table}`')

        if discard is not None:
            logging.info(f'Discarding columns `{discard}`')
            columns = list(self.execute_default_index(
                f'SHOW COLUMNS FROM `{table}`',).Field)
            columns = [col for col in columns if col not in discard]
            columns_str = json.dumps(columns).replace(
                "'", '`').replace('"', '`')[1:-1]
            return self.execute(f'SELECT {columns_str} FROM `{table}`')

        if isinstance(condition, dict):
            where_clause = []
            where_value_list = []
            for where_column, where_value in condition.items():
                where_clause.append(f'`{where_column}`=%s')
                where_value_list.append(where_value)
            where_clause_string = ' AND '.join(where_clause)
            return self.execute(f'SELECT * FROM `{table}` WHERE {where_clause_string}', params=where_value_list)

        return self.execute(f'SELECT * FROM `{table}`')

    def get_latest(self, data, group_by_col, sort_col):
        """
        Group data by a column containing repeated values and get latest from it by
        taking the latest value based on another column.

        Example:
        Get the latest products
            id     product   date
            220    6647     2014-09-01
            220    6647     2014-10-16
            826    3380     2014-11-11
            826    3380     2015-05-19
            901    4555     2014-09-01
            901    4555     2014-11-01

        The function will return
            id     product   date
            220    6647     2014-10-16
            826    3380     2015-05-19
            901    4555     2014-11-01

        Args:
            data (DataFrame): Pandas DataFrame to query on.
            group_by_col (str): Column containing repeated values.
            sort_col (str): Column to identify the latest record.

        Returns:
            (DataFrame) Contains the latest records. (None if an error occurs)
        """
        try:
            logging.info('Grouping data...')
            logging.info(f'Data: {data}')
            logging.info(f'Group by column: {group_by_col}')
            logging.info(f'Sort column: {sort_col}')
            return data.sort_values(sort_col).groupby(group_by_col).tail(1)
        except KeyError as e:
            logging.error(f'Column `{e.args[0]}` does not exist.')
            return None
        except:
            logging.exception('Something went wrong while grouping data.')
            return None
        
    def master_execute_(self, query, attempt=0, **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        try:
            if attempt == 0:
                query = self.convert_to_oracle(query)
            else:
                pass
            # query = self.convert_to_oracle(query)
            data = pd.read_sql(query, self.engine, **kwargs)
            self.engine.dispose()
            logging.info(f"Query: {query} data: {data} params: {kwargs}")
        except exc.ResourceClosedError:
            return True
        except exc.IntegrityError as e:
            logging.warning(f'Integrity Error - {e}')
            return None
        except (exc.StatementError, OperationalError) as e:
            logging.warning(
                f'Creating new connection. Engine/Connection is probably None. [{e}]')
            attempt += 1
            if attempt <= 5:
                self.connect()
                logging.debug(f'Attempt #{attempt}')
                return self.execute_(query, attempt=attempt, **kwargs)
            else:
                logging.debug(f'Maximum attempts reached. ({5})')
                return False
        except:
            logging.exception(
                f'Something went wrong while connecting. Check trace.')
            params = kwargs['params'] if 'params' in kwargs else None
            return False
        finally:
            self.engine.dispose()

        return data.replace({pd.NA: None})
