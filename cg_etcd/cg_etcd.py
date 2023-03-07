from etcd3.client import client
from functools import wraps
import importlib
from time import sleep
import logging

class ServiceConfig:
    def __init__(self, env: dict):
        for key, value in env.items():
            setattr(self, key, value)
class ETCD:

    """
    The ETCD class is a constructor that initializes an instance of an ETCD object. It takes various optional keyword arguments (kwargs) that can be used to configure the object.

    The parameters are:

        host (string)   : the hostname or IP address of the ETCD server (default: None)
        port (int)  : the port number to connect to (default: None)
        ca_cert (string)    : path to a PEM-encoded CA certificate file to use to authenticate the server (default: None)
        cert_key (string)   : path to a PEM-encoded private key file to use for client authentication (default: None)
        cert_cert (string)  : path to a PEM-encoded client certificate file to use for client authentication (default: None)
        grpc_options (string)    : dictionary of gRPC options to use when establishing a connection to the server (default: None)
        timeout (int)   : the number of seconds to wait before timing out on a connection attempt (default: None)
        user (string)   : the username to use for authentication (default: None)
        password (string)    : the password to use for authentication (default: None)
        max_retries (int)    : the maximum number of times to retry a connection if it fails (default: 3)
        retry_interval (int)    : the number of seconds to wait before retrying a failed connection attempt (default: 2)
        retry_count (int)   : variable that tracks the number of retries attempted by the ETCD client (default: 0)
        configs (dict)  : a dictionary to store the configuration values retrieved from the ETCD server (default: None)
        logger  : a logger object to use for logging

        connect() method is called in the constructor to establish a connection to the ETCD server.

    """

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
        self.max_retries = kwargs.get('max_retries', 3)
        self.retry_interval = kwargs.get('retry_interval', 2)
        self.retry_count = 0
        self.update_configs_function_path = kwargs.get('update_configs_function_path', 'app.settings import update_configs')
        self.configs = kwargs.get('configs', {})
        self.logger = logging.getLogger(__name__)
        self.connect()

    def handle_closed_connection(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "closed channel!" in str(e):
                    self = args[0]
                    self.logger.critical("ConnectionClosedError: %s", e)
                    self.retry_connection()
                else:
                    raise e
        return wrapper

    def connect(self):
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

    @handle_closed_connection
    def get_config(self, service_name: str):
        """
        Retrieves configuration values for a specified service name from an etcd 
        then stores them in a dictionary (configs)
        If no configuration values are found raises a ValueError.

        After retrieving the configuration values, the function sets up a 
        watch on the specified key prefix (service name) and invokes a callback function whenever 
        the values for the specified key prefix are updated.

        parameter:
            service_name (string)   : name of the service for which the configuration is being retrieved.

        """

        # adds a '/' to the end of the service name and uses it as a prefix to search for all keys with that prefix in the etcd server.
        service_name = service_name + '/'
        sequence_of_value_and_metadata_tuples = self._pool.get_prefix(service_name)

        for value in sequence_of_value_and_metadata_tuples:
            env_name = value[1].key.decode('utf-8')
            env_name = env_name.replace(service_name, '')
            env_value = value[0].decode('utf-8')
            self.configs[env_name] = env_value
            self.service_name = service_name
        
        self.data_class = ServiceConfig(self.configs)

        if not self.configs:
            raise ValueError(
                "Failed to retrieve config values for service_name %s", service_name)

        self.watch_key_prefix_service_name_and_callback(service_name)

        self.logger.info(
            "Retrieved config values for service_name %s", service_name)
        return self.data_class

    @handle_closed_connection
    def watch_key_prefix_service_name_and_callback(self, service_name: str):
        try:
            self._pool.add_watch_prefix_callback(
                key_prefix=service_name, callback=self.callback)
            self.logger.info(
                "Started watching service_name %s for changes", service_name)

        except Exception as e:
            self.logger.error(
                "Failed to start watching service_name %s: %s", service_name, str(e))

    def callback(self, response_or_err):
        try:
            env_name, env_value = (response_or_err.events[0].key.decode('utf-8'),
                                   response_or_err.events[0].value.decode('utf-8'))
            env_name = env_name.replace(self.service_name, '')
            self.configs[env_name] = env_value
            setattr(self.data_class, env_name, env_value)
            # # Split the string into its components
            # module_name, function_name = self.update_configs_function_path.split(" import ")
            # # Import the module
            # module = importlib.import_module(module_name)
            # # Get the function from the module
            # update_configs = getattr(module, function_name)
            # update_configs()

            self.logger.info("Updated config value for key %s", env_name)

        except Exception as e:
            self.logger.error(
                "Failed to update config value %s: %s", env_name, str(e))

    def close(self):
        self._pool.close()

    def retry_connection(self):
        try:
            while self.retry_count < self.max_retries:
                self.retry_count += 1
                self.logger.info(
                    f"Retrying connection, attempt {self.retry_count}...")
                try:
                    self.connect()
                    self.logger.info("Connection successful")
                    self.retry_count = 0
                    return
                except Exception as e:
                    self.logger.error(f"Connection failed: {e}")
                    sleep(self.retry_interval)
        except Exception as e:
            self.logger.critical("Max retries exceeded, exiting...")
