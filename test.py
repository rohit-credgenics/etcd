from cg_etcd.cg_etcd import ETCD

import pdb
pdb.set_trace()
data ={}
etcd = ETCD(host='localhost', port = 2379, data=data)
etcd.connect()
etcd.get_config( key_prefix='logo')


