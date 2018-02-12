"""Pycoki PostgreSQL implement"""

from pycoki import KeyValueStore
import psycopg2
from psycopg2.extras import DictCursor

class PgSQLKeyValueStore(KeyValueStore):
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
        return (namespace, key, value, timestamp, namespace, key, value, timestamp)

    @staticmethod
    def get_connection(connection_str):
        """Get connection by given connection string

        :param connection_str: Connection string
        :type connection_str: str
        :return: Connection
        :rtype: Connection
        """
        return psycopg2.connect(dsn=connection_str, cursor_factory=DictCursor)

    @staticmethod
    def get_sqls(table_name):
        """Get dictionary of SQLs called in methods of KeyValueStore

        :param table_name: Key-Value store table
        :type table_name: str
        :return: Dictionary of SQL
        :rtype: dict
        """
        return {
            "prepare_check": "SELECT relname FROM pg_class WHERE relkind='r' and relname='{0}';".format(table_name),
            "prepare_create": "create table {0} (kv_namespace VARCHAR(50), kv_key VARCHAR(100), kv_value VARCHAR(4000), kv_timestamp timestamp with time zone, primary key(kv_namespace, kv_key))".format(table_name),
            "get": "select kv_value from {0} where kv_namespace=%s and kv_key=%s".format(table_name),
            "get_all": "select kv_key, kv_value from {0} where kv_namespace=%s".format(table_name),
            "keys": "select kv_key from {0} where kv_namespace=%s".format(table_name),
            "set": """insert into {0} (kv_namespace, kv_key, kv_value, kv_timestamp) values (%s,%s,%s,%s) 
                    on conflict on constraint {0}_pkey
                    do update set kv_namespace=%s, kv_key=%s, kv_value=%s, kv_timestamp=%s""".format(table_name),
            "remove": "delete from {0} where kv_namespace=%s and kv_key=%s".format(table_name),
            "remove_all": "delete from {0} where kv_namespace=%s".format(table_name),
        }
