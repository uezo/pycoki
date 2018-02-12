"""Pycoki SQL Database implement"""

from pycoki import KeyValueStore
import pyodbc

class SQLDBKeyValueStore(KeyValueStore):
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
            "value": None if not "kv_value" in cols else row[cols.index("kv_value")],
            "namespace": None if not "kv_namespace" in cols else row[cols.index("kv_namespace")],
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
            "prepare_create": "create table {0} (kv_namespace NVARCHAR(50), kv_key NVARCHAR(100), kv_value NVARCHAR(4000), kv_timestamp DATETIME2, primary key(kv_namespace, kv_key))".format(table_name),
            "get": "select kv_value from {0} where kv_namespace=? and kv_key=?".format(table_name),
            "get_all": "select kv_key, kv_value from {0} where kv_namespace=?".format(table_name),
            "keys": "select kv_key from {0} where kv_namespace=?".format(table_name),
            "set": """
                    merge into {0} as A
                    using (select ? as kv_namespace, ? as kv_key, ? as kv_value, ? as kv_timestamp) as B
                    on (A.kv_namespace = B.kv_namespace and A.kv_key = B.kv_key)
                    when matched then
                    update set kv_value=B.kv_value, kv_timestamp=B.kv_timestamp
                    when not matched then 
                    insert (kv_namespace, kv_key, kv_value, kv_timestamp) values (B.kv_namespace, B.kv_key, B.kv_value, B.kv_timestamp);
                    """.format(table_name),
            "remove": "delete from {0} where kv_namespace=? and kv_key=?".format(table_name),
            "remove_all": "delete from {0} where kv_namespace=?".format(table_name),
        }
