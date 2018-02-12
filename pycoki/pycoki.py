"""Pycoki - Python Compatible Key-value-store Interface for databases

(Features)
- Simple and Easy: Focused on just getting/setting value(s) with a few preparations(serialization, encoding...) and configurations
- Compatibility: Same application interface when SQLite(default), MySQL, SQLServer and any other RDBMS is used for backend
- All data in one table: Same key for different values by using namespace(collection names, categories...)
"""

from datetime import datetime
import logging
import traceback
import sqlite3
import json
from pytz import timezone

DEFAULT_TABLE_NAME = "pycoki"
DEFAULT_CONNECTION_STR = "pycoki.db"

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            if obj.tzinfo:
                return obj.strftime("%Y-%m-%d %H:%M:%S %z")
            else:
                return obj.strftime("%Y-%m-%d %H:%M:%S")
        return super().default(obj)

class KeyValueStore:
    def __init__(self, namespace=None, logger=None, tzone=None, connection=None, close_connection=False, sqls=None):
        """Constractor of KeyValueStore
        Use KeyValueStore.open() method instead

        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param logger: Logger
        :type logger: logging.Logger
        :param tzone: Timezone
        :type tzone: timezone
        :param connection: Connection
        :type connection: Connection
        :param close_connection: Close connection when close method called
        :type close_connection: bool
        :param table_name: Key-Value store table
        :type table_name: str
        """
        self.sqls = sqls
        self.namespace = namespace
        self.logger = logger
        self.timezone = tzone
        self.connection = connection
        self.close_connection=close_connection

    def init_table(self, query_params=tuple(), connection=None):
        """Create new table if it doesn't exist

        :param query_params: Query parameters for checking table
        :type query_params: tuple
        :param connection: Connection
        :type connection: Connection
        """
        conn = connection if connection else self.connection
        try:
            cursor = conn.cursor()
            cursor.execute(self.sqls["prepare_check"], query_params)
            if cursor.fetchone() is None:
                cursor.execute(self.sqls["prepare_create"])
                conn.commit()
        except Exception as ex:
            self.logger.error("Error occured in initializing table: " + str(ex) + "\n" + traceback.format_exc())
        finally:
            cursor.close()

    def close(self):
        """Close connection if it was created from connection string
        """
        if self.close_connection:
            try:
                self.connection.close()
            except Exception as ex:
                self.logger.error("Error occured in closing connection: " + str(ex) + "\n" + traceback.format_exc())
        else:
            self.logger.info("Skipped closing connection")

    def get(self, key=None, namespace=None, connection=None):
        """Get value by key or all values in namespace

        :param key: Key
        :type key: str
        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param connection: Connection
        :type connection: Connection
        :return: Value or all values in namespace
        """
        ns = namespace if namespace else self.namespace
        conn = connection if connection else self.connection
        if not conn:
            self.logger.error("Connection is not available")
            return
        ret = None
        try:
            cursor = conn.cursor()
            if key:
                cursor.execute(self.sqls["get"], (ns, key))
                row = cursor.fetchone()
                if row is not None:
                    r = self.map_record(row)
                    ret = None if r["value"] is None or str(r["value"]) == "" else json.loads(str(r["value"]))
            else:
                ret = {}
                cursor.execute(self.sqls["get_all"], (ns, ))
                for row in cursor:
                    r = self.map_record(row)
                    ret[str(r["key"])] = None if r["value"] is None or str(r["value"]) == "" else json.loads(str(r["value"]))
        except Exception as ex:
            self.logger.error("Error occured in getting data from database: " + str(ex) + "\n" + traceback.format_exc())
        finally:
            cursor.close()
        return ret

    def keys(self, namespace=None, connection=None):
        """Get all keys in namespace

        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param connection: Connection
        :type connection: Connection
        :return: All keys in namespace
        :rtype: list
        """
        ret = []
        ns = namespace if namespace else self.namespace
        conn = connection if connection else self.connection
        if not conn:
            self.logger.error("Connection is not available")
            return
        try:
            cursor = conn.cursor()
            cursor.execute(self.sqls["keys"], (ns, ))
            for row in cursor:
                r = self.map_record(row)
                ret.append(str(r["key"]))
        except Exception as ex:
            self.logger.error("Error occured in getting keys from database: " + str(ex) + "\n" + traceback.format_exc())
        finally:
            cursor.close()
        return ret

    def set(self, key, value, namespace=None, connection=None):
        """Set value with key

        :param key: Key
        :type key: str
        :param value: Value
        :type value: object
        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param connection: Connection
        :type connection: Connection
        :return: Result
        :rtype: bool
        """
        ns = namespace if namespace else self.namespace
        conn = connection if connection else self.connection
        if not conn:
            self.logger.error("Connection is not available")
            return False
        try:
            serialized_value = "" if not value else json.dumps(value, cls=DateTimeJSONEncoder)
            cursor = conn.cursor()
            cursor.execute(self.sqls["set"], self.edit_params(ns, key, serialized_value, datetime.now(self.timezone)))
            conn.commit()
            return True
        except Exception as ex:
            self.logger.error("Error occured in saving data: " + str(ex) + "\n" + traceback.format_exc())
        finally:
            cursor.close()
        return False
    
    def remove(self, key=None, namespace=None, connection=None):
        """Remove value by key or all values in namespace

        :param key: Key
        :type key: str
        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param connection: Connection
        :type connection: Connection
        :return: Result
        :rtype: bool
        """
        ns = namespace if namespace else self.namespace
        conn = connection if connection else self.connection
        if not conn:
            self.logger.error("Connection is not available")
            return False
        try:
            cursor = conn.cursor()
            if key:
                cursor.execute(self.sqls["remove"], (ns, key))
            else:
                cursor.execute(self.sqls["remove_all"], (ns, ))
            conn.commit()
            return True
        except Exception as ex:
            self.logger.error("Error occured in removing data: " + str(ex) + "\n" + traceback.format_exc())
        finally:
            cursor.close()
        return False

    @staticmethod
    def edit_params(namespace, key, value, timestamp):
        """Edit SQL params

        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param key: Key
        :type key: str
        :param value: Value
        :type value: str
        :param timestamp: Timestamp
        :type timestamp: datetime
        :return: params
        :rtype: tuple
        """
        return (namespace, key, value, timestamp)

    @staticmethod
    def get_connection(connection_str):
        """Get connection by given connection string

        :param connection_str: Connection string
        :type connection_str: str
        :return: Connection
        :rtype: Connection
        """
        return None

    @staticmethod
    def map_record(row):
        """Map data from record to dict

        :param row: A row of record set
        :type row: sqlite3.Row
        :return: Record
        :rtype: dict
        """
        return {
            "key": None if not "kv_key" in row else row["kv_key"],
            "value": None if not "kv_value" in row else row["kv_value"],
            "namespace": None if not "kv_namespace" in row else row["kv_namespace"],
        }

    @staticmethod
    def get_sqls(table_name):
        """Get dictionary of SQLs called in methods of KeyValueStore

        :param table_name: Key-Value store table
        :type table_name: str
        :return: Dictionary of SQL
        :rtype: dict
        """
        return {}

class SQLiteKeyValueStore(KeyValueStore):
    @staticmethod
    def edit_params(namespace, key, value, timestamp):
        """Edit SQL params

        :param namespace: Namespace of Key-Value
        :type namespace: str
        :param key: Key
        :type key: str
        :param value: Value
        :type value: str
        :param timestamp: Timestamp
        :type timestamp: datetime
        :return: params
        :rtype: tuple
        """
        return (namespace, key, value, timestamp.strftime("%Y-%m-%d %H:%M:%S %z") if timestamp.tzinfo else timestamp.strftime("%Y-%m-%d %H:%M:%S"))

    @staticmethod
    def get_connection(connection_str):
        """Get connection by given connection string

        :param connection_str: Connection string
        :type connection_str: str
        :return: Connection
        :rtype: Connection
        """
        conn = sqlite3.connect(connection_str)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def map_record(row):
        """Map data from record to dict

        :param row: A row of record set
        :type row: sqlite3.Row
        :return: Record
        :rtype: dict
        """
        cols = row.keys()
        return {
            "key": None if not "kv_key" in cols else row["kv_key"],
            "value": None if not "kv_value" in cols else row["kv_value"],
            "namespace": None if not "kv_namespace" in cols else row["kv_namespace"],
        }

    @staticmethod
    def get_sqls(table_name):
        """Get dictionary of SQLs called in methods of KeyValueStore

        :param table_name: Key-Value store table
        :type table_name: str
        :return: Dictionary of SQL
        :rtype: dict
        """
        return {
            "prepare_check": "select * from sqlite_master where type='table' and name='{0}'".format(table_name),
            "prepare_create": "create table {0} (kv_namespace TEXT, kv_key TEXT, kv_value TEXT, kv_timestamp TEXT, primary key(kv_namespace, kv_key))".format(table_name),
            "get": "select kv_value from {0} where kv_namespace=? and kv_key=?".format(table_name),
            "get_all": "select kv_key, kv_value from {0} where kv_namespace=?".format(table_name),
            "keys": "select kv_key from {0} where kv_namespace=?".format(table_name),
            "set": "replace into {0} (kv_namespace, kv_key, kv_value, kv_timestamp) values (?,?,?,?)".format(table_name),
            "remove": "delete from {0} where kv_namespace=? and kv_key=?".format(table_name),
            "remove_all": "delete from {0} where kv_namespace=?".format(table_name),
        }


def start(connection_str=None, namespace=None, logger=None, tzone=None, connection=None, table_name=None, init_table=False, init_params=tuple(), kvsclass=None):
    """Get a new instance of KVS

    :param connection_str: Connection string
    :type connection_str: str
    :param namespace: Namespace of Key-Value
    :type namespace: str
    :param logger: Logger
    :type logger: logging.Logger
    :param tzone: Timezone
    :type tzone: timezone
    :param connection: Connection
    :type connection: Connection
    :param table_name: Key-Value store table
    :type table_name: str
    :param init_table: Create new table if it doesn't exist
    :type init_table: bool
    :return: Instance of KeyValueStore
    :rtype: KeyValueStore
    """
    cls = kvsclass if kvsclass else SQLiteKeyValueStore
    ret = cls(
        namespace=namespace if namespace else "__",
        logger=logger if logger else logging.getLogger(__name__),
        tzone=tzone if tzone else timezone("UTC"),
        connection=connection if not connection_str else cls.get_connection(connection_str),
        close_connection=False if not connection_str else True,
        sqls=cls.get_sqls(table_name if table_name else DEFAULT_TABLE_NAME)
    )
    if init_table:
        ret.init_table(query_params=init_params)
    return ret

def get(key=None, namespace=None, connection_str=None, init_table=True, init_params=tuple(), kvsclass=None):
    """Getter without instancing

    :param key: Key
    :type key: str
    :param namespace: Namespace of Key-Value
    :type namespace: str
    :param connection_str: Connection string
    :type connection_str: str
    :return: Value or all values in namespace
    """
    cls = kvsclass if kvsclass else SQLiteKeyValueStore
    try:
        temp = start(connection_str=connection_str if connection_str else DEFAULT_CONNECTION_STR, init_table=init_table, init_params=init_params, kvsclass=cls)
        return temp.get(key=key, namespace=namespace)
    finally:
        temp.close()

def set(key, value, namespace=None, connection_str=None, init_table=True, init_params=tuple(), kvsclass=None):
    """Setter without instancing

    :param key: Key
    :type key: str
    :param value: Value
    :type value: object
    :param namespace: Namespace of Key-Value
    :type namespace: str
    :param connection_str: Connection string
    :type connection_str: str
    :return: Result
    :rtype: bool
    """
    cls = kvsclass if kvsclass else SQLiteKeyValueStore
    try:
        temp = start(connection_str=connection_str if connection_str else DEFAULT_CONNECTION_STR, init_table=init_table, init_params=init_params, kvsclass=cls)
        return temp.set(key=key, value=value, namespace=namespace)
    finally:
        temp.close()
    return False
