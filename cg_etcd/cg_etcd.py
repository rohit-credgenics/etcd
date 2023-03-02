from etcd3.client import client
import logging

class ETCD:

    def __init__(self, **kwargs) -> None:
        self._pool = None
        self.host = kwargs.get('host', None)
        self.port = kwargs.get('port', None)
        self.ca_cert = kwargs.get('ca_cert', None)
        self.cert_key = kwargs.get('cert_key', None)
        self.cert_cert = kwargs.get('cert_cert', None)
        self.grpc_options = kwargs.get('grpc_options', None)
        self.timeout = kwargs.get('timeout', None)
        self.user = kwargs.get('user', None)
        self.password = kwargs.get('password', None)
        self.data = kwargs.get('data', None)
        self.logger = logging.getLogger(__name__)
        self.connect()

    def connect(self):
        try:
            self._pool = client(
                host=self.host,
                port=self.port,
                ca_cert=self.ca_cert,
                cert_key=self.cert_key,
                cert_cert=self.cert_cert,
                timeout=self.timeout,
                user=self.user,
                password=self.password,
                grpc_options=self.grpc_options
            )
            self.logger.info("Connected to etcd at %s:%d", self.host, self.port)
        except Exception as e:
            self.logger.error("Failed to connect to etcd at %s:%d: %s", self.host, self.port, str(e))
    

    def get_config(self, key_prefix: str):
        try:
            response = self._pool.get_prefix(key_prefix)
            for value in response:
                key = value[1].key.decode('utf-8')
                key = key.replace(key_prefix, '')
                value = value[0].decode('utf-8')
                self.data[key] = value
                self.key_prefix = key_prefix
                self.watch_key_prefix_and_callback(key_prefix)
            self.logger.info("Retrieved config values for key prefix %s", key_prefix)

        except Exception as e:
            self.logger.error("Failed to retrieve config values for key_prefix %s: %s", key_prefix, str(e))
        

    def watch_key_prefix_and_callback(self, key_prefix: str):
        try:
            self._pool.add_watch_prefix_callback(
                key_prefix=key_prefix, callback=self.callback)
            self.logger.info("Started watching key_prefix %s for changes", key_prefix)

        except Exception as e:
            self.logger.error("Failed to start watching key_prefix %s: %s", key_prefix, str(e))
    

    def callback(self, response_or_err):
        try:
            key, value = (response_or_err.events[0].key.decode('utf-8'), 
                        response_or_err.events[0].value.decode('utf-8'))
            key = key.replace(self.key_prefix, '')
            self.data[key] = value
            self.logger.info("Updated config value for key %s", key)

        except Exception as e:
            self.logger.error("Failed to update config value %s: %s", key, str(e))    
