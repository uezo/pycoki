"""Pycoki SQL Database implement"""

from pycoki import KeyValueStore
import pyodbc

class SQLDBKeyValueStore(KeyValueStore):
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
        return pyodbc.connect(connection_str)

    @staticmethod
    def map_record(row):
        """Map data from record to dict

        :param row: A row of record set
        :type row: pyodbc.Row
        :return: Record
        :rtype: dict
        """
        cols = [cd[0] for cd in row.cursor_description]
        return {
            "key": None if not "kv_key" in cols else row[cols.index("kv_key")],
            "value": None if not "value" in cols else row[cols.index("value")],
            "namespace": None if not "namespace" in cols else row[cols.index("namespace")],
        }

    @staticmethod
    def get_connection_str(host="", port=1433, database="pycokidb", user="", password="", driver="ODBC Driver 13 for SQL Server"):
        """
        :param host: Hostname
        :type host: str
        :param port: Port
        :type port: int
        :param database: Database
        :type database: str
        :param user: User name
        :type user: str
        :param password: Password
        :type password: str
        :param driver: ODBC Driver name
        :type driver: str
        :return: Connection string
        :rtype: str
        """
        return "DRIVER={{{5}}};SERVER={0};PORT={1};DATABASE={2};UID={3};PWD={4}".format(host, port, database, user, password, driver)


    @staticmethod
    def get_sqls(table_name):
        """Get dictionary of SQLs called in methods of KeyValueStore

        :param table_name: Key-Value store table
        :type table_name: str
        :return: Dictionary of SQL
        :rtype: dict
        """
        return {
            "prepare_check": "select id from dbo.sysobjects where id = object_id('{0}')".format(table_name),
            "prepare_create": "create table {0} (namespace NVARCHAR(50), kv_key NVARCHAR(100), value NVARCHAR(4000), timestamp DATETIME2, primary key(namespace, kv_key))".format(table_name),
            "get": "select value from {0} where namespace=? and kv_key=?".format(table_name),
            "get_all": "select kv_key, value from {0} where namespace=?".format(table_name),
            "keys": "select kv_key from {0} where namespace=?".format(table_name),
            "set": """
                    merge into {0} as A
                    using (select ? as namespace, ? as kv_key, ? as value, ? as timestamp) as B
                    on (A.namespace = B.namespace and A.kv_key = B.kv_key)
                    when matched then
                    update set value=B.value, timestamp=B.timestamp
                    when not matched then 
                    insert (namespace, kv_key, value, timestamp) values (B.namespace, B.kv_key, B.value, B.timestamp);
                    """.format(table_name),
            "remove": "delete from {0} where namespace=? and kv_key=?".format(table_name),
            "remove_all": "delete from {0} where namespace=?".format(table_name),
        }
