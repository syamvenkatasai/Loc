from datetime import datetime
from elasticsearch import Elasticsearch
import re
import os
from os import listdir, remove, system, getenv

from ace_logger import Logging


# es = Elasticsearch(
#     ['elasticsearch'],
#     http_auth=('user', 'secret'),
#     scheme="https",
#     port=443,
# )

logging = Logging()

logging.getLogger("elasticsearch").disabled = True
logging.getLogger("urllib3").disabled = True
logging.getLogger('urllib3.connectionpool').disabled = True

es_dns = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_DNS','')
es_port = os.environ.get('ELASTIC_SEARCH_FULL_PORT', '')
es_scheme = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_SCHEME','')

es = Elasticsearch(
    [f'{es_dns}'],
    http_auth=('elastic','MagicWord'),
    scheme=f"{es_scheme}",
    port=es_port,
)

RESERVED_CHARACTER = ['+', '-', '=', '&&', '||', '>', '<', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\', '/']

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


#
# def get_wildcard_query(text):
#     """
#     Author : Akshat Goyal
#
#     :return:
#     """
#     query = {
#         "query": {
#             "query_string": {
#                 "query": f"*{text}*",
#                 "fields": ["*"],
#                 "fuzziness": "Auto"
#             }
#         }
#     }
#
#     return query

def escape_special_character(query):
    return re.sub('(\+|\-|\=|&&|\|\||\>|\<|\!|\(|\)|\{|\}|\[|\]|\^|"|~|\*|\?|\:|\\\|\/)', '\\\\\\1', query)
# def escape_special_character(value):
#     if not value:
#         return value
#     for char in RESERVED_CHARACTER:
#         if char in value:
#             value = value.replace(char, f'\{char}')
#     return value

def get_range_query_equality(field, gte=None, lte=None):
    """
    Author : Akshat Goyal

    :param field: The field on which query is to performed
    :param lt: less than
    :param gt: greater than
    :return:
    """
    if lte and gte:
        # lte = escape_special_character(lte)
        # gte = escape_special_character(gte)
        range_query = {
            "range": {
                field: {
                    "gte": gte,
                    "lte": lte
                }
            }
        }
    elif lte:
        # lte = escape_special_character(lte)
        range_query = {
            "range": {
                field: {
                    "lte": lte
                }
            }
        }
    elif gte:
        # gte = escape_special_character(gte)
        range_query = {
            "range": {
                field: {
                    "gte": gte
                }
            }
        }
    else:
        range_query = {}

    return range_query


def get_wildcard_query(query, field='*', int=False, exact=True):
    """
    Author : Akshat Goyal

    :param field:
    :param query:
    :return:
    """
    search_query = {}
    if query:
        if exact:
            new_query = query
            if not int:
                new_query = escape_special_character(new_query)
            
            search_query = {
                "query_string": {
                    "query": f"\"{new_query}\"",
                    "fields": [field],
                    "default_operator": "AND"
                }
            }

        else:
            if not int:
                new_query = query.lower()
                new_query = escape_special_character(new_query)
                new_query = new_query.split()
                new_query = '* *'.join(new_query)
            else:
                new_query = query
            
            search_query = {
                "query_string": {
                    "query": f"*{new_query}*",
                    "fields": [field],
                    "default_operator": "AND"
                }
            }

    return search_query


def get_term_query(field, query):
    """
    Author :Akshat Goyal

    :param field:
    :param query:
    :return:
    """
    search_query = {}
    if field and query:
        query = escape_special_character(query)
        search_query = {
            "term": {field: query}
        }
    return search_query


def aggregate_query_should(queries):
    if not queries:
        return {}

    should_query = {"should": queries}

    return should_query


def aggregate_query_must(queries):
    if not queries:
        return {}

    must_query = {"must": queries}

    return must_query


def aggregate_query_must_not(queries):
    if not queries:
        return {}

    must_not_query = {"must_not": queries}

    return must_not_query


def aggregate_query_filter(queries):
    if not queries:
        return {}

    filter_query = {"filter": queries}

    return filter_query


def generate_bool_query(must={}, should={}, filter={}, must_not={}):
    """
    Author : Akshat Goyal

    :param must:
    :param should:
    :param filter:
    :return:
    """

    query = {}
    if must:
        query.update(must)
    if should:
        query.update(should)
    if filter:
        query.update(filter)
    if must_not:
        query.update(must_not)

    bool_query = {"bool": query}

    return bool_query


def search_data_with_json(index, body):
    """
    Author : Akshat Goyal

    :param index:
    :param body:
    :return:
    """
    res = es.search(index=index, body=body, request_timeout=1000)

    total = res['hits']['total']['value']
    result = []
    for hit in res['hits']['hits']:
        result.append(hit['_source'])

    return result, total


#
# def elasticsearch_free_text(text, sources=''):
#     """
#     Author : Akshat Goyal
#
#     :param text:
#     :param sources:
#     :return:
#     """
#     query = get_wildcard_query(text)
#
#     index = get_search_indexes(sources)
#
#     logging.info(f'searching with data index - {index}, query - {query}')
#     data = search_data_with_json(index, query)
#
#     return data
def get_value_query(field, value):
    value_query = {}
    if field and value:
        if isinstance(value, str):
            if '*' in value:
                value = value.replace('*', '')
                value = escape_special_character(value)
                value_query = get_wildcard_query(value, field, exact=False)
            else:
                value_query = get_wildcard_query(value, field)
        else:
            value_query = get_wildcard_query(value, field, int=True)

    return value_query


def get_must_not_query(filters):
    filter_queries = []
    for filter in filters:
        if filter.get('range', False):
            field = filter.get('field', None)
            start = filter.get('gte', None)
            end = filter.get('lte', None)

            if (start or end) and field:
                filter_queries.append(get_range_query_equality(field, start, end))
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, start or end')
        elif isinstance(filter.get('value', None), list):
            field = filter.get('field', None)
            value = filter.get('value', None)
            if field and value:
                new_values = []
                for v in value:
                    if type(v) == str:
                        v = escape_special_character(v)
                        new_values.append(v.lower())
                    else:
                        new_values.append(v)

                filter_queries.append({"terms": {field: new_values}})
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, value')
        else:
            field = filter.get('field', None)
            value = filter.get('value', None)
            if field and value:
                filter_queries.append(get_value_query(field, value))
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, value')

    filter_query = aggregate_query_must_not(filter_queries)

    return filter_query


def get_filter_query_old(filters):
    filter_queries = []
    for filter in filters:
        if filter.get('range', False):
            field = filter.get('field', None)
            start = filter.get('gte', None)
            end = filter.get('lte', None)

            if (start or end) and field:
                logging.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@START OR END DETECTED IN GET FILTER QUERY")
                filter_queries.append(get_range_query_equality(field, start, end))
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, start or end')
        elif isinstance(filter.get('value', None), list):
            field = filter.get('field', None)
            value = filter.get('value', None)
            if field and value:
                new_values = []
                for v in value:
                    if type(v) == str:
                        v = escape_special_character(v)
                        new_values.append(v.lower())
                    else:
                        new_values.append(v)

                filter_queries.append({"terms": {field: new_values}})
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, value')
        else:
            field = filter.get('field', None)
            value = filter.get('value', None)
            if field and value:
                filter_queries.append(get_value_query(field, value))
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, value')

    filter_query = aggregate_query_filter(filter_queries)

    return filter_query

# def get_filter_query(filters):
#     filter_queries = []
#     for filter in filters:
#         if filter.get('range', False):
#             field = filter.get('field', None)
#             start = filter.get('gte', None)
#             end = filter.get('lte', None)

#             if (start or end) and field:
#                 logging.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@START OR END DETECTED IN GET FILTER QUERY")
#                 filter_queries.append(get_range_query_equality(field, start, end))
#             else:
#                 logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
#                 logging.exception(f'parameters that has to be provided - field, start or end')
#         elif isinstance(filter.get('value', None), list):
#             field = filter.get('field', None)
#             value = filter.get('value', None)
#             if field and value:
#                 new_values = []
#                 for v in value:
#                     if type(v) == str:
#                         v = escape_special_character(v)
#                         new_values.append(get_value_query(field,v.lower()))
#                     else:
#                         new_values.append(get_value_query(field,v))

#                 filter_queries.append({"bool": {"should":new_values}})
                
                

#             else:
#                 logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
#                 logging.exception(f'parameters that has to be provided - field, value')
#         else:
#             field = filter.get('field', None)
#             value = filter.get('value', None)
#             if field and value:
#                 filter_queries.append(get_value_query(field, value))
#             else:
#                 logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
#                 logging.exception(f'parameters that has to be provided - field, value')

#     filter_query = aggregate_query_filter(filter_queries)

#     return filter_query

def get_filter_query(filters):
    filter_queries = []
    for filter in filters:
        logging.info(f"filter#####{filter}")
        if filter.get('range', False):
            field = filter['range'].get('field', None)
            start = filter['range'].get('gte', None)
            end = filter['range'].get('lte', None)
            logging.info(f"##field{field}#start{start}#{end}")


            if (start or end) and field:
                logging.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@START OR END DETECTED IN GET FILTER QUERY")
                filter_queries.append(get_range_query_equality(field, start, end))
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception('parameters that has to be provided - field, start or end')
        elif isinstance(filter.get('value', None), list):
            field = filter.get('field', None)
            value = filter.get('value', None)
            if field and value:
                new_values = []
                for v in value:
                    if type(v) == str:
                        v = escape_special_character(v)
                        new_values.append(get_value_query(field,v.lower()))
                    else:
                        new_values.append(get_value_query(field,v))

                filter_queries.append({"bool": {"should":new_values}})
                
                

            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, value')
        else:
            field = filter.get('field', None)
            value = filter.get('value', None)
            if field and value:
                filter_queries.append(get_value_query(field, value))
            else:
                logging.exception(f'un-sufficient parameters provided, arguments provided - {filter}')
                logging.exception(f'parameters that has to be provided - field, value')

    filter_query = aggregate_query_filter(filter_queries)

    return filter_query


def get_main_query(text, columns, use_columns=False):
    main_query = []
    if text and use_columns and columns:
        for column in columns:
            main_query.append(get_wildcard_query(text, column, exact=False))
    elif text:
        main_query.append(get_wildcard_query(text, exact=False))
    else:
        give_all = {
            "match_all": {}
        }
        main_query.append(give_all)

    return main_query


def insert_sort(elasticsearch_query, sort):
    elasticsearch_query.update({'sort': sort})
    return elasticsearch_query


def elasticsearch_search(input):
    """
    Author : Akshat Goyal

    :param input:
    :args start_point(optional): the starting offset from the starting of result list
    :args size(optional): the number of records to be given from starting_point
    :args text(optional): the text which should be there in the records in any field
    :args filter(optional): A list of filter that will applied
    :args source(optional): A list or str of index on which search is to be performed
    :args columns(optional): A list of columns which are to be returned
    :args use_column_search(required if columns not empty) : flag which determines if columns will be used for
                                                            query search or not,
                                                            Defaults to False
    :args tenant_id

    Note: for dates the format is yyyy-MM-dd'T'HH:mm:ss
    :return:
    """
    start_point = input.get('start_point', 0)
    size = input.get('size', 10)
    query = input.get('text', '')
    filters = input.get('filter', [])
    sources = input.get('source', '')
    columns = input.get('columns', [])
    sort = input.get('sort', [])
    tenant_id = input.get('tenant_id', '')
    use_column_search = input.get('use_column_search', False)
    must_nots = input.get('must_not', [])
    
    logging.info(f"####input#{input}")

    must_not_query = get_must_not_query(must_nots)

    filter_query = get_filter_query(filters)

    main_query = get_main_query(query, columns, use_column_search)

    should_query = aggregate_query_should(main_query)

    bool_query = generate_bool_query(should=should_query, filter=filter_query, must_not=must_not_query)

    elasticsearch_query = {"query": {}}
    elasticsearch_query['query'].update(bool_query)
    elasticsearch_query.update({"from": start_point, "size": size})
    elasticsearch_query.update({"min_score": 0.5})

    if columns:
        fields = {"_source": columns}
        elasticsearch_query.update(fields)

    index = get_search_indexes(sources, tenant_id)
    elasticsearch_query.update({'sort': sort})
    # elasticsearch_query = insert_sort(elasticsearch_query, sort)

    logging.info(f'searching with data index - {index}, query - {elasticsearch_query}')

    try:
        data = search_data_with_json(index, elasticsearch_query)
    except:
        return [], 0
    return data

def update(elastic_input):
    """
    """
    _id = elastic_input['id']
    sources = elastic_input.get('source', '')
    data = elastic_input['to_update']
    tenant_id = elastic_input['tenant_id']

    body = {"doc": data}

    index = get_search_indexes(sources, tenant_id)

    es.update(index=index, id=_id, body=body)

def delete(elastic_input):
    """
    """
    _id = elastic_input['id']
    sources = elastic_input.get('source', '')
    tenant_id = elastic_input['tenant_id']

    index = get_search_indexes(sources, tenant_id)

    es.delete(index=index,id=_id)

def insert(elastic_input):
    """
    """
    _id = elastic_input['id']
    sources = elastic_input.get('source', '')
    data = elastic_input['to_insert']
    tenant_id = elastic_input['tenant_id']

    index = get_search_indexes(sources, tenant_id)

    try:
        es.indices.create(index=index, ignore=400)
    except:
        pass

    es.index(index=index, id=_id, body=data)
