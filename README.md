Fork / Update of Mark Watson's [original version](https://code.google.com/a/apache-extras.org/p/hipy/)

The purpose of HiPy is to support programmatic construction of Hive queries in Python and easier management of queries, including queries with transform scripts.

HiPy enables grouping together in a single script of query construction, transform scripts and post-processing. This assists in traceability, documentation and re-usability of scripts. Everything appears in one place and Python comments can be used to document the script.

Hive queries are constructed by composing a handful of Python objects, representing things such as Columns, Tables and Select statements. During this process, HiPy keeps track of the schema of the resulting query output.

Transform scripts can be included in the main body of the Python script. HiPy will take care of providing the code of the script to Hive as well as of serialization and de-serialization of data to/from Python data types. If any of the data columns contain JSON, HiPy takes care of converting that to/from Python data types too.

## Differences from Upstream ##

- Is an actual module you can import / install from PyPI now
- Python 2.5 no longer supported
