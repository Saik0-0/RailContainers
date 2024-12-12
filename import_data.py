import numpy as np
import pyodbc


class DataImporter:
    def __init__(self):
        SERVER = 'MSI\\SQLEXPRESS'
        DATABASE = 'TZ'

        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
            f'=yes;')
        connection = pyodbc.connect(connection_string)
        self.cursor = connection.cursor()

    def import_table(self, string_to_import: str):
        table = string_to_import
        self.cursor.execute(table)
        if string_to_import == "SELECT * FROM dbo.upContainersVX":
            containers_array = np.array(self.cursor.fetchall(), dtype=object)
            # суммировали массу самого контейнера с массой груза
            containers_array[:, 3] += containers_array[:, 4]
            containers_array = np.delete(containers_array, 4, axis=1).astype(object)

            return containers_array

        table_array = self.cursor.fetchall()

        return table_array
