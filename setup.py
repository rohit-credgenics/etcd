from setuptools import setup

setup(
    name='cg_etcd',
    packages=['cg_etcd'],
    description='etcd Library for Credgenics',
    version='',
    url='',
    author='Rohit Choudhary',
    author_email='rohit.choudhary@credgenics.com',
    install_requires = [
        "etcd3==0.12.0",
    ]
)
