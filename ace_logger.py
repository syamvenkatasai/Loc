from py_zipkin.storage import get_default_tracer

import logging
import logging.config
import logging.handlers
import subprocess
import os

from inspect import getframeinfo, stack, currentframe


# from py_zipkin.storage import get_default_tracer

def get_container_id():
    return subprocess.check_output(['cat', '/etc/hostname']).decode('utf-8').replace('\\r\\n', '').replace('\\n','')

def sanitize_msg(msg):
    try:
        msg = ascii(msg)
        # msg = msg.__str__().encode('ascii', 'ignore').decode()
    except:
        msg = msg
    return msg


class Logging(logging.Logger):
    def __init__(self, name='container', **kwargs):
        self.log_levels = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
        }



        # os.makedirs('/logs/', mode=0o777, exist_ok=True
        if eval(os.environ.get('LOG_FAIL_SAFE', 'False')):
            logging_config = {
                'level': self.log_levels[os.environ['LOG_LEVEL']],
                'format': os.environ['LOG_FORMAT']
            }

            logging.basicConfig(**logging_config)
        # elif name is not None:
        else:

            container_id = get_container_id()
            #container_id = 'test_container'
            new_conf = '/log_config/.' + container_id + '.conf'
            if os.path.exists(new_conf):
                self.load_logging_conf(new_conf)
            else:
                container_name = name
                try:
                    with open('/log_config/logging.conf') as conf:
                        content = conf.read()
                        content = content.replace('$FILE_NAME', container_name)
                        new_conf = '/log_config/.' + container_id + '.conf'
                        with open(new_conf, 'w') as new_file:
                            new_file.write(content)
                        self.load_logging_conf(new_conf)
                except:
                    import traceback
                    traceback.print_exc()
                    print('error in logging, container_name = ', container_name)
        # else:
        #     container_id = get_container_id()
        #     new_conf = '/log_config/.' + container_id + '.conf'
        #     self.load_logging_conf(new_conf)


        # if os.path.exists('logging.conf'):
        #
        # else:
        #     logging.config.fileConfig('app/logging.conf')

        logging.getLogger('kafka').disabled = True
        logging.getLogger('kafka.client').disabled = True
        logging.getLogger('kafka.cluster').disabled = True
        logging.getLogger('kafka.conn').disabled = True
        logging.getLogger('kafka.consumer.fetcher').disabled = True
        logging.getLogger('kafka.consumer.group').disabled = True
        logging.getLogger('kafka.consumer.subscription_state').disabled = True
        logging.getLogger('kafka.coordinator').disabled = True
        logging.getLogger('kafka.coordinator.consumer').disabled = True
        logging.getLogger('kafka.metrics.metrics').disabled = True
        logging.getLogger('kafka.producer.kafka').disabled = True
        logging.getLogger('kafka.producer.record_accumulator').disabled = True
        logging.getLogger('kafka.producer.sender').disabled = True
        logging.getLogger('matplotlib').disabled = True
        logging.getLogger('matplotlib.font_manager').disabled = True
        logging.getLogger('requests').disabled = True
        logging.getLogger('urllib3.connectionpool').disabled = True
        logging.getLogger('werkzeug').disabled = True

        self.extra = {
            'tenantID': None,
            'traceID': None
        }

        self.set_ids(**kwargs)

    def load_logging_conf(self, file_path):
        logging.config.fileConfig(file_path)
        logging.getLogger().setLevel(
            self.log_levels[os.environ['LOG_LEVEL']])

    def set_ids(self):
        tenant_id = None
        trace_id = None
        line_no = None
        file_name = None
        current_func_name = None
        calledby_func_name = None

        try:
            # logging.debug('Setting tenant ID from zipkin...', extra=self.extra)

            zipkin_attrs = get_default_tracer().get_zipkin_attrs()
            #zipkin_attrs = None
            # logging.debug(f'zipkin_attrs: {zipkin_attrs}')
            if zipkin_attrs:
                tenant_id = zipkin_attrs.tenant_id
                trace_id = zipkin_attrs.trace_id

        except:
            message = 'Failed to get tenant and trace ID from zipkin header. Setting tenant/trace ID to None.'
            print(message)

        try:
            caller = getframeinfo(stack()[2][0])
            file_name = caller.filename
            line_no = caller.lineno
            current_func_name = caller.function
            calledby_func_name = 'Called By'
        except Exception as e:
            message = 'Failed to get caller stack'
            print('########',message, e)

        # logging.debug(f'Tenant ID: {tenant_id}', extra=self.extra)
        # logging.debug(f'Trace ID: {trace_id}', extra=self.extra)

        self.tenant_id = tenant_id
        self.trace_id = trace_id
        self.line_no = line_no
        self.file_name = file_name
        self.current_func_name = current_func_name
        self.calledby_func_name = calledby_func_name

        self.extra = {
            'tenantID': self.tenant_id,
            'traceID': self.trace_id,
            'fileName': self.file_name,
            'lineNo': self.line_no,
            'currentFuncName': self.current_func_name,
            'calledbyFuncName': self.calledby_func_name,
        }

    def basicConfig(self, *args, **kwargs):
        logging.basicConfig(**kwargs)

    def debug(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.debug(msg, extra=self.extra, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.info(msg, extra=self.extra, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.warning(msg, extra=self.extra, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.error(msg, extra=self.extra, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.critical(msg, extra=self.extra, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.exception(msg, extra=self.extra, *args, **kwargs)

    def getLogger(self, name=None):
        return logging.getLogger(name=name)

    def disable(self, level):
        logging.disable(level)
