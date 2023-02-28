from etcd3.client import client


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

    def get_config(self, key_prefix: str):
        response = self._pool.get_prefix(key_prefix)
        for value in response:
            key = value[1].key.decode()
            value = value[0].decode()
            self.data[key] = value

        self.watch(key_prefix=key_prefix)

    def watch(self, key_prefix: str):
        self._pool.add_watch_prefix_callback(
            key_prefix=key_prefix, callback=self.callback)

    def callback(self, response_or_err):
        key, value = (response_or_err.events[0].key.decode(), 
                      response_or_err.events[0].value.decode())
        self.data[key] = value
