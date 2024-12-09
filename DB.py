# db.py

import pyodbc
from pymupdf.mupdf import pdf_array_get_matrix
from pyodbc import Error

from myDB import DatabaseLogger, LOG_STR

db_logger = DatabaseLogger(LOG_STR)

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host_name};DATABASE={db_name};UID={user_name};PWD={user_password}')
        #connection=LoggedDBConnection(connection)  # Modifica la connessione per usare LoggedConnection
    except Error as e:
       print(f"Errore durante la connessione a MySQL: {e}")

    return connection

def execute_query(connection, query, params=None):
    _connection = create_connection('192.168.30.228', 'sistema', 'ITDev01', 'FlaskApp')
    cursor = _connection.cursor()
    frm = db_logger.format_query(query, params)
    try:
        cursor.execute(query, params)
        _connection.commit()
        db_logger.log_operation('W', frm)
        _connection.close()
    except Error as e:
       print(f"Errore durante l'esecuzione della query: {frm}\n{e}")
       _connection.close()

def execute_query2(connection, query, params=None):
    cursor = connection.cursor()
    frm = db_logger.format_query(query, params)
    try:
        cursor.execute(query, params)
        connection.commit()
        db_logger.log_operation('W', frm)
    except Error as e:
       print(f"Errore durante l'esecuzione della query: {frm} {e}")

def fetch_query_results2(connection, query, params=None):
    cursor = connection.cursor()
    frm = db_logger.format_query(query, params)
    results = None
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()

        db_logger.log_operation('R', frm)
        return results
    except Error as e:
       print(f"Errore durante il fetch dei risultati: {query} {e}")
       return None

def ReadData(query, params):
    return fetch_query_results(None, query, params)

def Execute(query, params=None):
    execute_query(None,query, params)

def fetch_query_results(connection, query, params=None):
    _connection = create_connection('192.168.30.228', 'sistema', 'ITDev01', 'FlaskApp')
    cursor = _connection.cursor()
    frm = db_logger.format_query(query, params)
    results = None
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()

        db_logger.log_operation('R', frm)
        _connection.close()
        return results
    except Error as e:
       print(f"Errore durante il fetch dei risultati: {frm} {e}")
       _connection.close()
       return results
#LOG_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.30.228;DATABASE=history;UID=sistema;PWD=ITDev01"
