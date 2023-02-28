from setuptools import setup, find_packages

setup(
    name='cg_etcd',
    packages=find_packages(),
    description='etcd Library for Credgenics',
    version='',
    url='',
    author='Rohit Choudhary',
    author_email='rohit.choudhary@credgenics.com',
    install_requires = [
        "etcd3==0.12.0","protobuf==3.19.6"
    ]
)