import pyodbc
from flask import Flask, session, request
from datetime import datetime

# Impostazioni della connessione al database
LOG_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.30.228;DATABASE=history;UID=sistema;PWD=ITDev01"

class DatabaseLogger:
    """
    Classe per gestire la connessione al database, eseguire operazioni di lettura e scrittura,
    e loggare le operazioni eseguite nel database con query completa inclusi i parametri.
    """
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def format_query(self, query, params):
        if not isinstance(params, (list, tuple)):
            params = (params,)

        formatted_query = query
        for param in params:
            formatted_value = f"'{param}'" if isinstance(param, str) else str(param)
            formatted_query = formatted_query.replace("?", formatted_value, 1)
        return formatted_query

    def execute_query(self, query, params=(), operation_type="READ"):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if operation_type == "READ":
                result = cursor.fetchall()
            else:
                conn.commit()
                result = None
            # Logga l'operazione con la query completa
            formatted_query = self.format_query(query, params)
            print(formatted_query)

            self.log_operation(operation_type, formatted_query)
            return result

    def read_query(self, query, params=()):
        return self.execute_query(query, params, operation_type="R")

    def write_query(self, query, params=()):
        self.execute_query(query, params, operation_type="W")

    def log_operation(self, operation_type, full_query):
        """
        Metodo per registrare un'operazione di lettura o scrittura nel log del database.
        """
