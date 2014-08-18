import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def requirements(fname):
    for line in open(os.path.join(os.path.dirname(__file__), fname)):
        yield line.strip()


setup(
    name="hipy",
    version='1.1',
    author="Mark Watson",
    author_email="watsonm@netflix.com",
    description=("Python Framework for Apache Hive"),
    license="Apache License 2.0",
    url="https://code.google.com/a/apache-extras.org/p/hipy/",
    py_modules=['HiPy'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
)
