"""Pycoki MySQL implement"""

from pycoki import KeyValueStore
import MySQLdb
from MySQLdb.cursors import DictCursor

class MySQLKeyValueStore(KeyValueStore):
    @staticmethod
    def serialize_date(dt):
        return dt

    @staticmethod
    def get_connection(connection_str):
        """Get connection by given connection string

        :param connection_str: Connection string
        :type connection_str: str
        :return: Connection
        :rtype: Connection
        """
        connection_str = connection_str if connection_str else "host=localhost;user=root;passwd=;db=pycokidb;charset=utf8;"
        connection_info = {"cursorclass": DictCursor, "charset": "utf8"}
        param_values = connection_str.split(";")
        for pv in param_values:
            if "=" in pv:
                p, v = list(map(str.strip, pv.split("=")))
                connection_info[p] = v
        return MySQLdb.connect(**connection_info)

    @staticmethod
    def map_record(row):
        """Map data from record to dict

        :param row: A row of record set
        :type row: dict(MySQL row)
        :return: Record
        :rtype: dict
        """
        return {
            "key": None if not "kv_key" in row else row["kv_key"],
            "value": None if not "value" in row else row["value"],
            "namespace": None if not "namespace" in row else row["namespace"],
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
            "prepare_check": "select * from information_schema.TABLES where TABLE_NAME='{0}' and TABLE_SCHEMA=%s".format(table_name),
            "prepare_create": "create table {0} (namespace VARCHAR(50), kv_key VARCHAR(100), value VARCHAR(4000), timestamp DATETIME, primary key(namespace, kv_key))".format(table_name),
            "get": "select value from {0} where namespace=%s and kv_key=%s".format(table_name),
            "get_all": "select kv_key, value from {0} where namespace=%s".format(table_name),
            "keys": "select kv_key from {0} where namespace=%s".format(table_name),
            "set": "replace into {0} (namespace, kv_key, value, timestamp) values (%s,%s,%s,%s)".format(table_name),
            "remove": "delete from {0} where namespace=%s and kv_key=%s".format(table_name),
            "remove_all": "delete from {0} where namespace=%s".format(table_name),
        }
