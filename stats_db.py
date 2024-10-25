#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:40:37 2019

@author: user
"""

from sqlalchemy import create_engine, exc
from MySQLdb._exceptions import OperationalError
from time import time
import pandas as pd
import os

class Stats_db:
    def __init__(self, database = 'stats', host=None, user=None, password=None, port='3306',tenant_id = None):
        host = os.environ['HOST_IP']
        user = f'{tenant_id}_{database}'
        #password =f'{tenant_id}_{database}'
        password = os.environ['LOCAL_DB_PASSWORD']
        port = os.environ['LOCAL_DB_PORT']
        if tenant_id == 'None':
            tenant_db = f'{database}'
        else:
            tenant_db = f'{tenant_id}_{database}'
        
        DIALECT = os.environ.get('DIALECT','oracle')
        SQL_DRIVER = os.environ.get('SQL_DRIVER', 'cx_oracle')
        SERVICE = os.environ.get('DATABASE_SERVICE','orcl')
        self.HOST = host
        self.PORT = port
        self.DATABASE_RAW = f'{tenant_id}_{database}'
        #self.PASSWORD = f'{tenant_id}_{database}'
        self.PASSWORD = password

        ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + self.DATABASE_RAW + ':' + \
            self.PASSWORD + '@' + self.HOST + ':' + \
            str(self.PORT) + '/?service_name=' + SERVICE
        print(ENGINE_PATH_WIN_AUTH)
        engine = create_engine(ENGINE_PATH_WIN_AUTH,pool_recycle=300, max_identifier_length=128)
        self.db_ = engine

        
        try:
            self.engine = self.db_
            try:
                self.connection = self.engine.connect()
            except Exception as e:
                print("Unable to connect to Database", e)                
        except Exception as e:
            print("Unable to create engine", e)

    def convert_to_mssql(self, query):
        query = query.replace('"', "'")
        inds = [i for i in range(len(query)) if query[i] == '`']
        for pos, ind in enumerate(inds):
            if pos % 2 == 0:
                query = query[:ind] + '"' + query[ind+1:]
            else:
                query = query[:ind] + '"' + query[ind + 1:]

        query = query.replace('%s', ':name')

        return query
                
    
    def get_stats_master(self, attempt=0):
        

        try:
            if attempt == 0:
                query = 'SELECT * FROM stats_master order by id'
                query = self.convert_to_mssql(query)
            else:
                query = 'SELECT * FROM stats_master'
            print(f'In get stats master {self.engine}')
            print(f"query is {query}")
            result_proxy = pd.read_sql(query, self.engine)
            print(f'Query after: {query}')
            print(f'Engine: {self.engine}')
        except exc.ResourceClosedError:
            print('Query does not have any value to return.')
            return True
        except exc.IntegrityError as e:
            print(f'Integrity Error - {e}')
            return None
        except (exc.StatementError, OperationalError) as e:
            print(
                f'Creating new connection. Engine/Connection is probably None. [{e}]')
            attempt += 1
            if attempt <= 5:
                self.connect()
                print(f'Attempt #{attempt}')
                return self.get_stats_master(attempt=attempt)
            else:
                print(f'Maximum attempts reached. ({5})')
                return False
        except:
            print('Something went wrong executing query. Check trace.')
            return False
        print(result_proxy)
        print("****")
        d, a = {}, []
        for indx, row in result_proxy.iterrows():
            for column, value in row.items():
                d = {**d, **{column: value}}
            a.append(d)

        print('>>>>>>>>>')
        print(a)
        stats_master_df = pd.DataFrame(a)
        #Renaming columns x,y to X,Y - Oracle issue
        stats_master_df.rename(columns={"x": "X", "y": "Y"}, inplace= True)
        print(stats_master_df)
        print(list(stats_master_df.columns))
        return stats_master_df

    def get_active_stats(self, attempt=0):
        # result_proxy = self.connection.execute(f'SELECT * FROM active_stats')

        print(f'In get active stats {self.engine}')
        # result_proxy = pd.read_sql(query, self.engine)

        try:
            if attempt == 0:
                query = 'SELECT * FROM active_stats'
                query = self.convert_to_mssql(query)
            else:
                query = 'SELECT * FROM "active_stats"'

            print(f'In get active master {self.engine}')
            print(f"query is {query}")
            result_proxy = pd.read_sql(query, self.engine)
            print(f'Query after: {query}')
            print(f'Engine: {self.engine}')
        except exc.ResourceClosedError:
            print('Query does not have any value to return.')
            return True
        except exc.IntegrityError as e:
            print(f'Integrity Error - {e}')
            return None
        except (exc.StatementError, OperationalError) as e:
            print(
                f'Creating new connection. Engine/Connection is probably None. [{e}]')
            attempt += 1
            if attempt <= 5:
                self.connect()
                print(f'Attempt #{attempt}')
                return self.get_stats_master(attempt=attempt)
            else:
                print(f'Maximum attempts reached. ({5})')
                return False
        except:
            print('Something went wrong executing query. Check trace.')
            return False
        print(result_proxy)
        print('^^^^')
        d, a = {}, []
        for indx, row in result_proxy.iterrows():
            for column, value in row.items():
                d = {**d, **{column: value}}
            a.append(d)
        print('*******')
        print(a)

        active_stats_df = pd.DataFrame(a)
        print(active_stats_df)
        return active_stats_df
    
    def active_stats(self):
        stats_master_df = self.get_stats_master()
        active_stats_df = self.get_active_stats()
        self.close_db_object()
        return pd.merge(stats_master_df, active_stats_df, on = 'id', how = 'inner').to_dict(orient = 'records')

    def close_db_object(self):
        
        self.engine.dispose()
