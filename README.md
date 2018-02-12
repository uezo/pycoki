# Pycoki

Python Compatible Key-value-store Interface for databases

## Features
- Simple and Easy: Focused on just getting/setting value(s) with a few preparations(serialization, encoding...) and configurations
- Compatibility: Same application interface when SQLite(default), MySQL, SQLServer, PostgreSQL and any other RDBMS is used for backend
- All data in one table: Same key for different values by using namespace(collection names, categories...)

## Installation

To install Pycoki, use pip.

```
$ pip install git+https://github.com/uezo/pycoki
```

## Quick start

Just `pycoki.start()` then you can get/set data. The backend DB is SQLite by default.

```python
# Import Pycoki
import pycoki

# Start Pycoki
p = pycoki.start("test.db", init_table=True)   #init_table is required for the first access to create data table

# Set data
p.set("key1", "value1")
p.set("key2", 123.45)
p.set("key3", {"sub_key1": "sub_val2", "sub_key2": 678, "sub_key3": {"dict_key1":"dict_val1", "dict_key2":["array_val1", "array_val2", "array_val3"]}})

# Get value by key
print(p.get("key1"))
# Get object value
print(p.get("key3")["sub_key3"]["dict_key2"][1])
# Get all values
print(p.get())
# Get keys
print(p.keys())

# Finish Pycoki
p.close()
```

```
value1
array_val2
{'key1': 'value1', 'key2': 123.45, 'key3': {'sub_key2': 678, 'sub_key3': {'dict_key1': 'dict_val1', 'dict_key2': ['array_val1', 'array_val2', 'array_val3']}, 'sub_key1': 'sub_val2'}}
['key1', 'key2', 'key3']
```

Or quick getter/setter without instancing and finalizing is like below:

```python
# Quick setter
pycoki.set("key4", "value4", connection_str="test.db")

# Quick getter
val = pycoki.get("key4", connection_str="test.db")
print(val)
```

```
value4
```

## Use MySQL

Switch the backend database to MySQL. To use this feature `MySQLdb` is required.

```python
# Import Pycoki and MySQL extension
import pycoki
from pycoki.mysql import MySQLKeyValueStore

# Start Pycoki
mysql_conn_str = "host=localhost;user=root;passwd=;db=pycokidb;charset=utf8;"
# p = pycoki.start(mysql_conn_str, kvsclass=MySQLKeyValueStore)
p = pycoki.start(mysql_conn_str, kvsclass=MySQLKeyValueStore, init_table=True, init_params=("pycokidb",))  #First access

# Set and get data
p.set("key1", "Hello MySQL!")
print(p.get("key1"))

# Finish Pycoki
p.close()
```

```
Hello MySQL!
```


## Use SQL Server / Azure SQL Database

Switch the backend database to SQL Server / Azure SQL Database. To use this feature `pyodbc` is required.

```python
# Import Pycoki and SQLDatabase extension
import pycoki
from pycoki.sqldb import SQLDBKeyValueStore

# Start Pycoki
sqldb_conn_str = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=******.database.windows.net;PORT=1433;DATABASE=******;UID=******;PWD=******"
# p = pycoki.start(sqldb_conn_str, kvsclass=SQLDBKeyValueStore)
p = pycoki.start(sqldb_conn_str, kvsclass=SQLDBKeyValueStore, init_table=True)  #First access

# Set and get data
p.set("key1", "Hello SQL Server!")
print(p.get("key1"))

# Finish Pycoki
p.close()
```

```
Hello SQL Server!
```


## Use PostgreSQL

Switch the backend database to PostgreSQL. To use this feature `psycopg2` is required.

```python
# Import Pycoki and SQLDatabase extension
import pycoki
from pycoki.pgsql import PgSQLKeyValueStore

# Start Pycoki
pgsql_conn_str = "postgresql://username:password@hostname:port/pycokidb"

# p = pycoki.start(pgsql_conn_str, kvsclass=PgSQLKeyValueStore)
p = pycoki.start(pgsql_conn_str, kvsclass=PgSQLKeyValueStore, init_table=True)  #First access

# Set and get data
p.set("key1", "Hello PostgreSQL!")
print(p.get("key1"))

# Finish Pycoki
p.close()
```

```
Hello PostgreSQL!
```

