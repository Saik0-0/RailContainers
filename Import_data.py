import numpy as np
import pyodbc


class DataImporter:
    def import_containers_vx(self, session_id: int):
        SERVER = 'MSI\\SQLEXPRESS'
        DATABASE = 'TZ'

        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
            f'=yes;')
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        containers = "SELECT * FROM dbo.upContainersVX"
        cursor.execute(containers)
        containers_array = np.array(cursor.fetchall(), dtype=object)
        # суммировали массу самого контейнера с массой груза
        containers_array[:, 3] += containers_array[:, 4]
        containers_array = np.delete(containers_array, 4, axis=1).astype(object)

        return containers_array

    def import_platforms_vx(self, session_id: int):
        SERVER = 'MSI\\SQLEXPRESS'
        DATABASE = 'TZ'
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
            f'=yes;')
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        platforms = 'SELECT * FROM dbo.upPlatformsVX'
        cursor.execute(platforms)
        platforms_array = cursor.fetchall()

        return platforms_array

    def import_containers_spr(self):
        SERVER = 'MSI\\SQLEXPRESS'
        DATABASE = 'TZ'
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
            f'=yes;')
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        containers_rules = 'SELECT * FROM dbo.upContainersSPR'
        cursor.execute(containers_rules)
        containers_rules_array = cursor.fetchall()

        return containers_rules_array

    def import_rules_spr(self):
        SERVER = 'MSI\\SQLEXPRESS'
        DATABASE = 'TZ'
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
            f'=yes;')
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        containers_on_platforms_rules = 'SELECT * FROM dbo.upRulesSPR'
        cursor.execute(containers_on_platforms_rules)
        containers_on_platforms_rules_array = cursor.fetchall()

        return containers_on_platforms_rules_array

    def import_tables_spr(self):
        SERVER = 'MSI\\SQLEXPRESS'
        DATABASE = 'TZ'
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
            f'=yes;')
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        containers_on_platforms_tables = 'SELECT * FROM dbo.upTablesSPR'
        cursor.execute(containers_on_platforms_tables)
        containers_on_platforms_tables_array = cursor.fetchall()

        return containers_on_platforms_tables_array
