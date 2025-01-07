import subprocess
import logging
import time
import os
import sys
import pyodbc
import datetime
from dotenv import load_dotenv 

class Logger:
    def __init__(self, path, log_name):
        self.path = path
        self.log_name = log_name
        self.__setup_logging()
    
    class PrintToLog:
        def write(self, message):
            if message.strip():
                logging.info(message.strip())

        def flush(self):
            pass
    
    def __get_log_filename(self):
        return datetime.datetime.now().strftime(f"{self.log_name}_%Y-%m-%d_00.00.00") + ".log"

    def __setup_logging(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        log_filename = os.path.join(self.path, self.__get_log_filename())
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            handlers=[
                                logging.FileHandler(log_filename, mode='a'),
                                logging.StreamHandler(sys.stdout)
                            ])
        
        sys.stdout = self.PrintToLog()
        sys.stderr = self.PrintToLog()


class Connection:
    def __init__(self, server, database, username, password, driver='{ODBC Driver 17 for SQL Server}', timeout=1800):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.timeout = timeout
        self.connection = self.connect()

    def connect(self):
        conn_str = (
            f'DSN={self.database};'
            f'UID={self.username};'
            f'PWD={self.password}'
        )
        try:
            conn = pyodbc.connect(conn_str)
            conn.execute("SET LOCK_TIMEOUT {}".format(self.timeout * 1000))
            conn.execute("SET QUERY_GOVERNOR_COST_LIMIT {}".format(self.timeout))
            conn.execute("SET NOCOUNT ON")
            conn.execute("SET ARITHABORT ON")
            print(f"Conexión exitosa a {self.database}.")
            return conn
        except Exception as e:
            print(f"Error al conectar a SQL Server: {e}")
            raise

    def run_query(self, query, return_data=True):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query.replace("\n", " "))
                while cursor.nextset():
                    pass
                if return_data:
                    return cursor.fetchall()
                else:
                    self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                raise
    
    def raise_email_error(self, message, subject="Error"):
        query = f"""EXEC {self.database}.DBO.SP_GR_PRO_MAIL @CODPER = 'ENVTYE', @DIREML = '', @DIRECC = '', @DIRCCO = '', @VARIABLES = '<ERROR>|{message.replace("'", " ")}#<ASUNTO>|{subject}', @ADJUNTOS = ''"""
        self.run_query(query, False)
    
    def close(self):
        self.connection.close()

def main():
    env_path = os.path.join(os.path.dirname(sys.executable), '.env')
    load_dotenv(env_path)

    path_log = os.getenv('PATH_LOG')
    log_name = os.getenv('LOG_NAME')
    logger = Logger(path_log, log_name)

    base = os.getenv('BASE_PRODUCTIVA')
    server = os.getenv('SERVER')
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')
    connection = Connection(server, base, username, password, timeout=1800)

    try:
        connection.run_query(f"EXEC SP_CO_GEN_PRECARGAS_TYE", return_data=False)
        print("Se ejecutó la generación de pre-cargas en Softland.")
    except Exception as e:
        print(f"Error al ejecutar la generación de pre-cargas en Softland: {e}")
        connection.raise_email_error(f"Error al ejecutar la generación de pre-cargas en Softland: {e}")
    finally:
        connection.close()

    print(f"Fin de la ejecución sft_precar.exe ...")
    print(f"-----------------------------------")

if __name__ == "__main__":
    main()
