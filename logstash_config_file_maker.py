import argparse
import os
import ast

from os import listdir, remove, system, getenv
from os.path import isfile, join
from elasticsearch import Elasticsearch

from flask_cors import CORS
from flask import Flask, jsonify, request, redirect, url_for

from ace_logger import Logging
from db_utils import DB
from making_view_for_logstash import what_a_view

logging = Logging(name='logstash_api')
from elasticsearch_utils import elasticsearch_search

# es = Elasticsearch(["elasticsearchfullsearch"], scheme="http", port=9200)
# es = Elasticsearch(
#     [os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_DNS','')],
#     scheme=os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_SCHEME',''),
#     port=os.environ.get('ELASTIC_SEARCH_FULL_PORT',''),
# )

es_dns = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_DNS','')
es_port = os.environ.get('ELASTIC_SEARCH_FULL_PORT', '')
es_scheme = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_SCHEME','')
tenants_tx_tables_view = os.environ.get('tenants_tx_tables_view', [])
if len(tenants_tx_tables_view) > 0:
    tenants_tx_tables_view = ast.literal_eval(tenants_tx_tables_view)

logging.info(f"########################## ESF dns {es_dns}")
logging.info(f"########################## ESF dns type {type(es_dns)}")
logging.info(f"########################## ESF port {es_port}")
logging.info(f"########################## ESF port {type(es_port)}")
#es_port = int(es_port)
logging.info(f"########################## ESF scheme {es_scheme}")
logging.info(f"########################## ESF scheme {type(es_scheme)}")

es = Elasticsearch(
    [f'{es_dns}'],
    http_auth=('elastic','MagicWord'),
    scheme=f"https",
    port=es_port,
    ca_certs="/usr/share/logstash/config/elasticsearch-ca.pem",
)

# from py_zipkin.zipkin import zipkin_span,ZipkinAttrs, create_http_headers_for_new_span

app = Flask(__name__)
CORS(app)

db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD']
}

def delete_files(path):
    [remove(join(path, f)) for f in listdir(path) if isfile(join(path, f))]
    return True

#create_master_config(master_tables,tenant_ids, master_tables_config, topath)
def create_master_config(master_tables,tenant_ids, config_file_path, config_to_path):
    full_pipline = []
    pipeline_template = ''
    with open(config_file_path, 'r') as config_file:
        boiler_plate = config_file.read()

        
        with open(pipeline_path, 'r') as pl:
            pipeline_template = str(pl.read())
        for tenant_id in tenant_ids:
            for table in master_tables:
                config = str(boiler_plate)
                config = config.replace('${MASTER_TABLE}', table)
                file_index = table.replace('.', '').replace('_', '')
                config = config.replace('${MASTER_TABLE_INDEX}', file_index)
                config = config.replace('${TENANT_ID_INDEX}', tenant_id)
                with open(join(config_to_path, tenant_id+table+".conf"), 'w') as write_file:
                    write_file.write(config)

                file_name = tenant_id+table+".conf"
                tenant_pipline = str(pipeline_template)
                tenant_pipline = tenant_pipline.replace('{pipelinename}', file_name)
                tenant_pipline = tenant_pipline.replace('{file_name}', file_name)
                full_pipline.append(tenant_pipline)

    full_pipline = '\n\r'.join(full_pipline)
    with open(join('/app/pipeline', 'pipelines.yml'), 'w') as pipeline:
        pipeline.write(full_pipline)

#create_logstash_config(files, tenant_ids, mypath, topath, pipeline_path)
def create_logstash_config(files, tenant_ids, mypath, topath, pipeline_path):
    full_pipline = []
    for file_path, file in files:
        with open(file_path, 'r') as f:
            boiler_plate = f.read()
            tenant_specific = ''
            pipeline_template = ''
            with open(pipeline_path, 'r') as pl:
                pipeline_template = str(pl.read())

            for temp_tenant_id in tenant_ids:
                tenant_specific = str(boiler_plate)
                tenant_specific = tenant_specific.replace('${TENANT_ID}', temp_tenant_id)
                tenant_id = temp_tenant_id.replace('.', '').replace('_', '').lower()
                tenant_specific = tenant_specific.replace('${TENANT_ID_INDEX}', tenant_id)
                with open(join(topath, tenant_id + file), 'w') as write_file:
                    write_file.write(tenant_specific)
                file_name = tenant_id + file
                tenant_pipline = str(pipeline_template)
                tenant_pipline = tenant_pipline.replace('{pipelinename}', file_name)
                tenant_pipline = tenant_pipline.replace('{file_name}', file_name)
                full_pipline.append(tenant_pipline)

    # for the logstash file
    tenant_pipline = str(pipeline_template)
    tenant_pipline = tenant_pipline.replace('{pipelinename}', 'logstash')
    tenant_pipline = tenant_pipline.replace('{file_name}', 'logstash.conf')
    full_pipline.append(tenant_pipline)

    full_pipline = '\n\r'.join(full_pipline)
    with open(join('/app/pipeline', 'pipelines.yml'), 'a+') as pipeline:
        pipeline.write(full_pipline)


def create_tenant_view(tenant_ids):
    db_table_mapping = {'extraction': ['ocr'], 'queues': ['queue_list']}
    main_table = {'queues': ['process_queue']}
    view_name = 'all_data'

    for tenant_id in tenant_ids:
        # tenant_id = temp_tenant_id.replace('.','')
        what_a_view.view_maker(main_table, db_table_mapping, view_name, tenant_id=tenant_id)

def create_tenant_view_all_tx_tables(tenants_tx_tables_view):
    
    for tenant_id in tenants_tx_tables_view:
        db_config['tenant_id'] = tenant_id
        tx_master_db = DB('tx_master', **db_config)
        
        query = "select * from master_upload_tables"
        query_data = tx_master_db.execute(query)

        table_names = query_data['table_name'].str.split('.')
        db_table_mapping = {}

        for name in table_names:
            if len(name) == 2 and name[-1] != 'process_queue':
                key, value = name
                if key in db_table_mapping:
                    db_table_mapping[key].append(value)
                else:
                    db_table_mapping[key] = [value]

        main_table = {'queues': ['process_queue']}
        view_name = 'all_data_tx_tables'

        # tenant_id = temp_tenant_id.replace('.','')
        what_a_view.view_maker(main_table, db_table_mapping, view_name, tenant_id=tenant_id)


def get_search_indexes(sources, temp_tenant_id=''):
    """
    Author : Akshat Goyal

    :param sources:
    :return:
    """
    tenant_id = temp_tenant_id.replace('.', '')
    if not sources:
        return '_all'
    indexes = []
    if isinstance(sources, list):
        for source in sources:
            new_source = tenant_id + source if tenant_id else source
            new_source = new_source.replace('.', '').replace('_', '').lower()
            indexes.append(new_source)
    elif isinstance(sources, str):
        new_source = tenant_id + '_' + sources if tenant_id else sources
        new_source = new_source.replace('.', '').replace('_', '').lower()
        indexes.append(new_source)

    return indexes


def create_index(tenant_ids, sources=(), master_sources=()):
    body = {
        'settings': {
            'analysis': {
                'analyzer': {
                    'default': {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "date_detection": "false",
            "numeric_detection": "false"
        }
    }
    body_date_gen = {
        'settings': {
            'analysis': {
                'analyzer': {
                    'default': {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "last_updated": {
                    "type": "date"
                }
            },
            "date_detection": "false",
            "numeric_detection": "false"
        }
    }


    body_with_date = {
        'settings': {
            'analysis': {
                'analyzer': {
                    'default': {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "@version": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ocr": {
                    "properties": {
                        "created_date": {
                            "type": "date"
                        },
                        "last_updated": {
                            "type": "date"
                        }
                    }
                },
                "process_queue": {
                    "properties": {
                        "created_date": {
                            "type": "date"
                        },
                        "last_updated": {
                            # "type": "date",
                            "type": "text",
                        },
                        "freeze": {
                            "type": "boolean"
                        },
                        "case_lock":{
                            "type": "boolean"
                        }
                    }
                },
                "ocr":{
                    "properties":{
                        "created_date":{
                            "type": "date"
                        },
                        "last_updated":{
                            "type": "date"
                        }
                    }
                }
            },
            "date_detection": "false",
            "numeric_detection": "false"
        }
    }
    indexes = []

    for tenant_id in tenant_ids:
        indexes.extend(get_search_indexes(sources, tenant_id))

    for ind in indexes:
        logging.debug(f'creating index - {ind}')
        es.indices.delete(index=ind, ignore=[400, 404])
        if 'processqueue' in ind:
            es.indices.create(index=ind, body=body_with_date, ignore=400)
        else:
            es.indices.create(index=ind, body=body, ignore=400)

    if master_sources:
        indexes = []

        for tenant_id in tenant_ids:
            indexes.extend(get_search_indexes(master_sources, tenant_id))

        for ind in indexes:
            es.indices.delete(index=ind, ignore=[400, 404])
            es.indices.create(index=ind, body=body_date_gen, ignore=400)


@app.route('/logstash_making_config', methods=['POST', 'GET'])
def logstash_making_config():
    # env_path = '../.env'
    # load_dotenv(dotenv_path=env_path)

    mypath = '/app/full_search_config/config_maker'
    topath = '/app/full_search_config/config'
    pipeline_path = '/app/full_search_config/pipeline_maker/pipelines.yml'
    delete_files(topath)
    # with open("../.env", "r") as file:
    #     contents = file.readlines()
    #     for line in contents:
    #         if "TENANT_ID" in line:
    #             value = line.split("=")[1]
    #             tenant_ids = value.replace('\n', "").split(';')
    # if not tenant_ids:
    #     tenant_ids = getenv("TENANT_ID")
    #     tenant_ids = tenant_ids.split(';')
    tenant_ids = os.environ['TENANT_ID']
    tenant_ids = tenant_ids.split(';')
    #      print(tenant_ids)

    files = [[join(mypath, f), f] for f in listdir(mypath) if isfile(join(mypath, f))]

    print(tenant_ids)
    create_logstash_config(files, tenant_ids, mypath, topath, pipeline_path)

    create_tenant_view(tenant_ids)
    if len(tenants_tx_tables_view) > 0 :
        create_tenant_view_all_tx_tables(tenants_tx_tables_view)
    create_index(tenant_ids, ['process_queue', "activedirectory"])


@app.route('/delete_row_not_in_db', methods=['POST', 'GET'])
def delete_row_not_in_db():
    data = request.json

    sources = data['sources']
    tenant_id = data['tenant_id']
    database_name = data['database_name']

    indexes = get_search_indexes(sources, tenant_id)

    for idx, source in enumerate(sources):
        body = {"query":{"bool":{"should":[{"match_all":{}}]}}}


        files = es.search(index=indexes[idx], body=body, request_timeout=1000)
        # files, total_files = elasticsearch_search(elastic_input)

        db_config['tenant_id'] = tenant_id
        db = DB(database_name, **db_config)
        query = f'select * from {source}'

        db_files = db.execute_(query)

        to_delete = []

        for file in files['hits']['hits']:
            file_id = file['_id']

            if db_files[db_files['id'] == int(file_id)].empty:
                to_delete.append(file_id)

        delete_query = {
            "query" : {
                "terms" : {
                    "_id" :
                      to_delete
                }
            }
        }

        logging.debug(f'body - {delete_query}')
        if to_delete:
            es.delete_by_query(index=indexes[idx], body=delete_query)

    return jsonify({'flag': True, 'message': "The index has been Neuralyzed"})




if __name__ == '__main__':

    mypath = '/app/full_search_config/config_maker'
    topath = '/app/full_search_config/config'
    pipeline_path = '/app/full_search_config/pipeline_maker/pipelines.yml'

    master_tables_config = '/app/full_search_config/master_config/master_config.conf'

    delete_files(topath)
    # with open("../.env", "r") as file:
    #     contents = file.readlines()
    #     for line in contents:
    #         if "TENANT_ID" in line:
    #             value = line.split("=")[1]
    #             tenant_ids = value.replace('\n', "").split(';')
    # if not tenant_ids:
    #     tenant_ids = getenv("TENANT_ID")
    #     tenant_ids = tenant_ids.split(';')
    tenant_ids = os.environ['TENANT_ID']
    tenant_ids = tenant_ids.split(';')
    #      print(tenant_ids)

    files = [[join(mypath, f), f] for f in listdir(mypath) if isfile(join(mypath, f))]

    print(tenant_ids)

    master_tables = os.environ['MASTER_TABLE'].split(';')
    create_master_config(master_tables,tenant_ids, master_tables_config, topath)

    create_logstash_config(files, tenant_ids, mypath, topath, pipeline_path)

    create_tenant_view(tenant_ids)
    if len(tenants_tx_tables_view) > 0 :
        create_tenant_view_all_tx_tables(tenants_tx_tables_view)
    create_index(tenant_ids, ['process_queue', "activedirectory"],master_tables)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port Number', default=5001)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')
    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, port=port, debug=True)
