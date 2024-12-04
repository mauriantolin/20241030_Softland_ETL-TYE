import subprocess
import logging
import time
import os
import sys
import pyodbc
import requests
import datetime
import re
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

class Script:
    def __init__(self, path):
        self.path = path

    def run(self):
        try:
            result = subprocess.run(self.path, check=True, text=True, capture_output=False)
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al ejecutar el script {self.path}: {e}")
        
class Connection:
    def __init__(self, server, database, username, password, driver='{ODBC Driver 17 for SQL Server}'):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.connection = self.connect()

    def connect(self):
        conn_str = f'DSN={self.database};UID={self.username};PWD={self.password}'
        #conn_str = f'SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};DRIVER={self.driver}'
        try:
            conn = pyodbc.connect(conn_str)
            print(f"Conexión exitosa a {self.database}.")
            return conn
        except Exception as e:
            print(f"Error al conectar a SQL Server: {e}")
            raise

    def run_query(self, query, return_data=True):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query.replace("\n", " "))
                if return_data:
                    return cursor.fetchall()
                else:
                    self.connection.commit()
            except Exception as e:
                self.connection.rollback()

    def raise_email_error(self, message, subject="Error"):
        query = f"""EXEC AKAPOLSA.DBO.SP_GR_PRO_MAIL @CODPER = 'ENVTYE', @DIREML = '', @DIRECC = '', @DIRCCO = '', @VARIABLES = '<ERROR>|{message.replace("'", " ")}#<ASUNTO>|{subject}', @ADJUNTOS = ''"""
        self.run_query(query, False)
    
    def close(self):
        self.connection.close()

class Item:
    def __init__(self, conn, inicia, ctacte, period, nromov, nroitm, oletye, tipren, nrotye):
        self.inicia = inicia
        self.ctacte = ctacte
        self.period = period
        self.nromov = nromov
        self.nroitm = nroitm
        self.oletye = oletye
        self.tipren = tipren
        self.nrotye = nrotye
        self.conn = conn
        self.file_path = ""

    
    def save_pdf(self, apikey, path_pdf):
        headers = {
        "X-Api-key": apikey
        }
        
        response = requests.get(self.oletye, headers=headers)
        if response.status_code == 200:
            try:
                # Extrae la extensión del archivo del enlace
                extension_match = re.search(r'\.([a-zA-Z0-9]+)$', self.oletye)
                if extension_match:
                    extension = extension_match.group(1)
                else:
                    extension = "unknown"  # Si no se puede determinar la extensión

                file_name = f"{self.ctacte}_{self.period}_{self.nromov}_{self.nroitm}.{extension}"
                folder_path = os.path.join(path_pdf, f'{self.ctacte}', f'{self.period}', f'{self.nromov}', f'{self.nroitm}')

                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
                self.file_path = os.path.join(folder_path, file_name)

                if not os.path.exists(self.file_path):
                    with open(self.file_path, 'wb') as file:
                        file.write(response.content)
                    print(f"Archivo guardado como {self.file_path}")
            except Exception as e:
                print(f"Error al guardar el archivo del gasto {self.ctacte} {self.period} {self.nromov} {self.nroitm}: {e}")
                self.conn.raise_email_error(f"Error al guardar el archivo del gasto {self.ctacte} {self.period} {self.nromov} {self.nroitm}: {e}.")       
                
    def update_pdf(self):
        self.conn.run_query(f"""
                EXEC SP_CO_REND_UPDATE_OLEOLE
                    @FLPATH = '{self.file_path}'
                    , @TIPREN = {self.tipren}
                    , @NROTYE = {self.nrotye}
                    , @NROITM = {self.nroitm}
                    """, False)

class Pdf:
    def __init__(self, conn, api_key, path_pdf):
        self.conn = conn
        self.api_key = api_key
        self.path_pdf = path_pdf
        self.items = self.get_pdf_objects()

    def get_pdf_objects(self):
        item_pdf_obj = []
        if self.conn:
            item_sql = self.conn.run_query(f"EXEC SP_CO_REND_GET_OLEOLE")
            item_pdf_obj = [Item(self.conn, *item) for item in item_sql]
        return item_pdf_obj
    
    def update_pdfs(self):
        for item in self.items:
            item.save_pdf(self.api_key, self.path_pdf)
            item.update_pdf()

def main():

    env_path = os.path.join(os.path.dirname(sys.executable), '.env')
    load_dotenv(env_path)
    
    path_log = os.getenv('PATH_LOG')
    log_name = os.getenv('LOG_NAME')
    logger = Logger(path_log, log_name)

    base = os.getenv('BASE_TYE')
    server = os.getenv('SERVER')
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')
    conn = Connection(server, base, username, password)
    path_pdf = os.getenv('PATH_PDF')

    api_key = os.getenv('API_KEY')

    try:
        pdfs = Pdf(conn, api_key, path_pdf)
        pdfs.get_pdf_objects()
        pdfs.update_pdfs()
    except Exception as e:
        print(f"Error al ejecutar la inserción de datos en Softland: {e}")
        conn.connection.rollback()
    finally:
        conn.close()


    path_app = os.getenv('PATH_APP')
    filename = os.path.join(path_app, 'sft_precar.exe')
    
    print(f"Fin de la ejecución pdf.exe ...")
    print(f"-----------------------------------")

    try: 
        script = Script(filename)
        script.run()
    except Exception as e:
        print(f"Error al ejecutar el script {filename}: {e}")
    
if __name__ == "__main__":
    main()
