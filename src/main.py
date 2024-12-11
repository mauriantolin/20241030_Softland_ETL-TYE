import json
import random
import time
import subprocess
import logging
import datetime
import os
import sys
import pyodbc
import requests
import xmltodict
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
        
class Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.connection.cursor()

    def execute(self, query, return_data=True):
        self.cursor.execute(query)
        if return_data:
            return self.cursor.fetchall()
        else:
            return
        
    def commit(self):
        self.cursor.commit()

    def rollback(self):
        self.cursor.rollback()

    def close(self):
        self.cursor.close()

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
            logging.info(f"Conexión exitosa a {self.database}.")
            return conn
        except Exception as e:
            logging.error(f"Error al conectar a SQL Server: {e}")
            raise

    def run_query(self, query, return_data=True):
        with self.connection.cursor() as cursor:
            cursor.execute(query.replace("\n", " "))
            if return_data:
                return cursor.fetchall()
            else:
                self.connection.commit()
        
    def raise_email_error(self, message, subject="Error"):
        query = f"""EXEC AKAPOLSA.DBO.SP_GR_PRO_MAIL @CODPER = 'ENVTYE', @DIREML = 'MAN@AKAPOL.COM', @DIRECC = '', @DIRCCO = '', @VARIABLES = '<ERROR>|{message.replace("'", " ")}#<ASUNTO>|{subject}', @ADJUNTOS = ''"""
        self.run_query(query, False)
    
    def close(self):
        self.connection.close()

class CashAdvance:
    def __init__(self, advance):
        self.nrotye = advance.get("Number", "")
        self.type = 4
        self.date = advance.get("Date", "")[:6]
        self.approver_legajo = next(
            (approver["Legajo"] for approver in advance.get("Approver", []) if approver.get("isFinanceRole") == "false"),
            advance["User"].get("Legajo", "")
        )
        self.user_legajo = advance["User"].get("Legajo", "")
        self.user_costcenter = advance["User"].get("CostCenter", [""])[0]
        self.user_name = advance["User"].get("Name", "")
        self.user_email = advance["User"].get("Email", "")
        self.amount = float(advance.get("Amount", 0))
        self.currency = advance.get("Currency", "")
        self.nromov = 0

    def __str__(self):
        return f"{self.nrotye} - {self.type} - {self.date} - {self.approver_legajo} - {self.user_legajo} - {self.user_costcenter} - {self.user_name} - {self.user_email} - {self.amount}"

class Costcenter:
    def __init__(self, costcenter):
        self.costcenter = costcenter
        self.rl = costcenter.get("CostCenter", [""])[0]
        self.amount = float(costcenter.get("Amount", 0))
        self.rp, self.codigo_vinc = self.__parse_rp()
        self.approver_legajo = costcenter.get("Approver", {}).get("Legajo", "")
        self.nroitp = 0

    def __parse_rp(self):
        rp = ""
        codigo_vinc = ""
        for alloc in self.costcenter.get("Allocation", []):
            code, item_code = alloc.get("Code", ""), alloc.get("Item", {}).get("Code", "")
            if code == "RP":
                rp = item_code
            elif code == "COD.VINC." and item_code != 'NA':
                codigo_vinc = item_code
        return rp, codigo_vinc

class Expense:
    def __init__(self, expense):
        self.expense = expense
        self.nrotye = expense.get("Number", "")
        self.date = expense.get("Date", "")
        self.account = "SE00058" if expense.get("ExpenseType") == "tip" else expense.get("Account", "")
        self.expense_type = "PROPINAS" if expense.get("ExpenseType") == "tip" else expense.get("ExpenseType", "")
        self.currency = expense.get("Currency", "")
        self.amount = float(expense.get("Amount", 0))
        self.comment = "" if expense.get("ExpenseType") == "tip" else re.sub(r"\W+", " ", expense.get("Comment", ""))
        self.receipt_link = "" if expense.get("ExpenseType") == "tip" else expense.get("Receipt", "") if expense.get("Receipt") is not None else ""
        self.recognized = self.truth_validation(expense.get("Unrecognized", ""))
        self.personal = self.truth_validation(expense.get("Personal", ""))
        self.reimburs = self.truth_validation(expense.get("Reimbursable", ""))
        self.approver_legajo = ""
        self.tax = self.expense.get("Tax", {})
        self.ticket_number = self.tax.get("TicketNumber", "")
        self.receipt_type = self.tax.get("ReceiptType", "")
        self.cuit = self.tax.get("Cuit", "")
        self.provider = self.tax.get("Merchant", "")
        self.letter = self.tax.get("Letter", "")
        self.location = self.tax.get("Location", "")
        self.total_costcenter = 0
        self.costcenters = self.__parse_costcenters()
        self.nroitm = 0

    def __parse_costcenters(self):
        costcenters = []
        for costcenter in self.expense.get("CostCenter", []):
            instance_costcenter = Costcenter(costcenter)
            costcenters.append(instance_costcenter)
            self.approver_legajo = instance_costcenter.approver_legajo or self.approver_legajo
            self.total_costcenter += float(instance_costcenter.amount)
            #total_costcenter
        return costcenters
    
    @staticmethod
    def truth_validation(value):
        return "S" if value == "true" else "N"

class Report:
    def __init__(self, report):
        self.report = report
        self.nrotye = report.get("Number", "")
        self.type = report.get("Type", "")
        self.date = report.get("Period", report.get("Date", ""))[:6]
        self.user_legajo = report["User"].get("Legajo", "")
        self.user_costcenter = report["User"].get("CostCenter", [""])[0]
        self.user_name = report["User"].get("Name", "")
        self.user_email = report["User"].get("Email", "")
        self.card_type = self.__get_card_type()
        self.total_cashadvance = sum(float(ca.get("ReportedAmountMD", 0)) for ca in report.get("CashAdvance", []))
        self.advance_numbers = [ca.get("Number", "") for ca in report.get("CashAdvance", [])]
        self.total_report = 0
        self.approver_legajo = ""
        self.expenses = self.__parse_expenses()
        self.nromov = 0
        self.cursor = None

    def __parse_expenses(self):
        expenses = []
        for expense in self.report.get("Expense", []):
            instance_expense = Expense(expense)
            self.total_report += instance_expense.amount
            self.approver_legajo = instance_expense.approver_legajo or self.user_legajo
            expenses.append(instance_expense)
            if instance_expense.total_costcenter != instance_expense.amount:
                print(f"Error: Total costcenter does not match total report for expense {expense['Number']}: {instance_expense.total_costcenter} != {instance_expense.amount}")
        return expenses
    
    def __get_card_type(self):
        return {"VISA SIGNATURE": 'S',
                "VISA CORPORATE": 'C',
                "VISA PURCHASING": 'P'
                }.get(self.report.get("CreditCard"), "")
    
    def __str__(self):
        return f"{self.nrotye} - {self.type} - {self.date} - {self.user_legajo} - {self.user_costcenter} - {self.user_name} - {self.user_email} - {self.card_type} - {self.total_cashadvance} - {self.total_report} - {self.approver_legajo}"

class WebService():
    def __init__(self, url, api_key):
        self.url = url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "X-Api-Key": self.api_key
        })
        self.fields_list = ("Allocation", "CostCenter", "Expense", "CashAdvance", "Report")
        self.response = self.__get_information_from_tye()
        self.cash_advances = self.__parse_cash_advances()
        self.reports = self.__parse_reports()

    def send_soap_request(self, xml):
        update_soap = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tye="http://tyeexpress.com/">
           <soapenv:Header/>
           <soapenv:Body>
              <tye:RegisterDocuments>
                 <tye:xml>
                    {xml}
                 </tye:xml>
                 <tye:apiKey>{self.api_key}</tye:apiKey>
              </tye:RegisterDocuments>
           </soapenv:Body>
        </soapenv:Envelope>
        """ 

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://tyeexpress.com/RegisterDocuments'
        }
        response = self.session.post(self.url, data=update_soap.replace("\n", ""), headers=headers)

        if response.status_code == 200:
            logging.info('Actualización de la rendición hecha correctamente.')
        else:
            logging.error(f'Error al actualizar la rendición: {response.status_code}')
        return response.status_code

    def __get_information_from_tye(self):
        body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tye="http://tyeexpress.com/">
            <soapenv:Header/>
            <soapenv:Body>
                <tye:GetInformation>
                    <tye:apiKey>{self.api_key}</tye:apiKey>
                </tye:GetInformation>
            </soapenv:Body>
            </soapenv:Envelope>
            """

        response = self.session.post(self.url, data=body)
        response = xmltodict.parse(response.text, force_list=self.fields_list, dict_constructor=dict)
        return response

    def response_message(self, method):
        return self.response["soap:Envelope"]["soap:Body"][f"{method}Response"][f"{method}Result"]["Message"]["Code"] == "0"

    def __parse_cash_advances(self):
        cash_advances = []
        response = self.response["soap:Envelope"]["soap:Body"]["GetInformationResponse"]["GetInformationResult"].get("CashAdvance",{})
        for advance in response:
            if advance["User"]["Legajo"] != "null":
                cash_advance_instance = CashAdvance(advance)
                cash_advances.append(cash_advance_instance)
                logging.info(cash_advance_instance)
        return cash_advances
    
    def __parse_reports(self):
        reports = []
        response = self.response["soap:Envelope"]["soap:Body"]["GetInformationResponse"]["GetInformationResult"].get("Report", "")
        for report in response:
            if report["User"]["Legajo"] != "null":
                report_instance = Report(report)
                reports.append(report_instance)
                logging.info(report_instance)
        return reports

class Inserter:
    def __init__(self, connection, web_service):
        self.connection = connection
        self.web_service = web_service

    def cashadvance_insert(self):
        for advance in self.web_service.cash_advances:
            try:
                advance.nromov = self.connection.run_query(f"""
                    EXEC SP_CO_REND_MAX_CORRTH 
                        @INICIA = '{advance.user_legajo}'
                        , @PERIOD = {advance.date}
                """)[0][0]
                advance_query = f"""
                        EXEC SP_CO_REND_INS_CORRTH 
                        @INICIA = '{advance.user_legajo}'
                        , @PERIOD = {advance.date}
                        , @NROMOV = {advance.nromov}
                        , @NROSFT = NULL
                        , @NROTYE = {advance.nrotye}
                        , @TIPREN = {advance.type}
                        , @MONEDA = '{advance.currency}' 
                        , @IMPORT = {advance.amount}
                        , @IMPANT = 0
                        , @USRAUT = '{advance.approver_legajo}'
                        , @TARJET = ''
                        """
                self.connection.run_query(advance_query, False)
                logging.info(f"|_Registro C - {advance.nrotye} insertado: {1}")
            except Exception as e:
                logging.error(f"Error al insertar datos C: {e}")
                #error

    def __costcenter_insert(self,report, expense):
        for k, costcenter in enumerate(expense.costcenters, 1):
            costcenter.nroitp = k
            subitem_query = f"""
                EXEC SP_CO_REND_INS_CORRTP
                    @TIPREN = {report.type}
                    , @NROTYE = {report.nrotye}
                    , @CTACTE = '{report.user_legajo}'
                    , @PERIOD = {report.date}
                    , @NROMOV = {report.nromov}
                    , @NROITM = {expense.nroitm}
                    , @NROITP = {costcenter.nroitp}
                    , @CODIRL = '{costcenter.rl[:6]}'
                    , @CODIRP = '{costcenter.rp[:6]}'
                    , @CODVIN = '{costcenter.codigo_vinc[:10]}'
                    , @IMPORT = {costcenter.amount}
                """
            report.cursor.execute(subitem_query, False)
            logging.info(f"|___Registro P - {costcenter.rl}|{costcenter.rp} insertado: {1}")

    def __expense_insert(self, report):
        for i, expense in enumerate(report.expenses, 1):
            expense.nroitm = i
            expense_tipcom = expense.receipt_type if expense.letter != "" else "DI" if expense.receipt_link == None else "DIC"
            item_query = f"""
                EXEC SP_CO_REND_INS_CORRTI 
                    @TIPREN = {report.type}
                    , @NROTYE = {report.nrotye}
                    , @CTACTE = '{report.user_legajo}'
                    , @PERIOD = {report.date}
                    , @NROMOV = {report.nromov}
                    , @NROITM = {expense.nroitm}
                    , @TIPCOM = '{expense_tipcom}'
                    , @NROORI = '{expense.ticket_number}'
                    , @FCHMOV = '{expense.date}'
                    , @IMPORT = {expense.amount}
                    , @MONEDA = '{expense.currency}'
                    , @CUENTA = ''
                    , @CODIRL = '{expense.costcenters[0].rl[:6]}'
                    , @CODIRP = '{expense.costcenters[0].rp[:6]}'
                    , @CODVIN = '{expense.costcenters[0].codigo_vinc[:10]}'
                    , @JURISD = '{expense.location}'
                    , @NOMBRE = '{expense.provider}'
                    , @NRODOC = '{expense.cuit}'
                    , @OLEOLE = ''
                    , @OLETYE = '{expense.receipt_link}'
                    , @ARTCOD = '{expense.account}'
                    , @CONCEP = '{expense.expense_type}'
                    , @OBSERV = '{expense.comment}'
                    , @NORECO = '{expense.recognized}'
                    , @PERSON = '{expense.personal}'
                    , @REEMBO = '{expense.reimburs}'
                    """
            report.cursor.execute(item_query, False)
            logging.info(f"|___Registro I - {expense.nrotye} insertado: {1}")
            self.__costcenter_insert(report, expense)

    def advance_update(self, advance_numbers):
        """Updates multiple cash advances with the report number they're associated with"""
        try:
            for advance_number in advance_numbers:
                update_query = f"""
                    EXEC SP_CO_REND_UPDATE_ANTICI 
                        @NROANT = {advance_number},
                        @NROTYE = {self.report.nrotye}
                """
                self.connection.run_query(update_query, False)
                logging.info(f"|_Advance {advance_number} acutalizado para rendicion {self.report.nrotye}")
        except Exception as e:
            logging.error(f"Error updating advances for report {self.report.nrotye}: {e}")
            raise

    def report_insert(self):
        for report in self.web_service.reports:
            try:
                report.cursor = Cursor(self.connection)
                report.nromov = report.cursor.execute(f"""
                    EXEC SP_CO_REND_MAX_CORRTH 
                        @INICIA = '{report.user_legajo}'
                        , @PERIOD = {report.date}
                    """)[0][0]
                report_query = f"""
                    EXEC SP_CO_REND_INS_CORRTH 
                    @INICIA = '{report.user_legajo}'
                    , @PERIOD = {report.date}
                    , @NROMOV = {report.nromov}
                    , @NROSFT = NULL
                    , @NROTYE = {report.nrotye}
                    , @TIPREN = {report.type}
                    , @MONEDA = ''
                    , @IMPORT = {report.total_report}
                    , @IMPANT = {report.total_cashadvance}
                    , @USRAUT = '{report.approver_legajo}'
                    , @TARJET = '{report.card_type}'
                    """
                report.cursor.execute(report_query, False)

                if report.advance_numbers:
                    self.advance_update(report.advance_numbers)
                    
                logging.info(f"|_Registro H - {report.nrotye} insertado: {1}")
                self.__expense_insert(report)
                report.cursor.commit()
            except Exception as e:
                if e.args[0] == '23000' and report.type == 2:
                    logging.error(f"La rendición de tarjeta {report.nrotye} está a la espera de ser procesada.")
                else:
                    logging.error(f"Error al insertar datos  H - {report.nrotye}: {e}")
                report.cursor.rollback()
                #error

class Notifier:
    def __init__(self, nrotye, tipren, nrosft, importe, compag, noveda, ctacte, impant):
        self.nrotye = nrotye
        self.tipren = tipren
        self.nrosft = nrosft
        self.importe = importe
        self.compag = compag
        self.noveda = 0 if noveda == None else noveda
        self.ctacte = ctacte
        self.impant = impant
        self.document = "CashAdvance" if self.tipren == 4 else "Report"
        self.news = {
            0: {
                1: self.noveda == 0 and self.tipren == 1,
                2: self.noveda == 0 and self.tipren == 2,
                4: self.noveda == 0 and self.tipren == 4
                }, 
            1: {
                1: self.noveda == 1 and self.tipren == 1 and self.nrosft != None and self.ctacte != None and (self.importe <= self.impant or self.compag != None),
                4: self.noveda == 1 and self.tipren == 4 and self.nrosft != None and self.ctacte != None and self.compag != None 
                }
            }
        self.new = self.generate_new()
        
    def get_new_validation(self):
        return self.news.get(self.noveda, {}).get(self.tipren, False)

    def generate_new(self):
        if self.get_new_validation() == True:
            return f"""<tye:{self.document}>
                            <tye:Type>{self.noveda + 1}</tye:Type>
                            <tye:Number>{self.nrotye}</tye:Number>
                            <tye:Document>
                                <tye:Company>AKAPOL</tye:Company>
                                <tye:DocumentNumber>{self.noveda + 1}{self.tipren}{self.nrotye}</tye:DocumentNumber>
                                <tye:FiscalYear>{datetime.date.today().year}</tye:FiscalYear>
                                <tye:DocumentDate>{datetime.date.today().strftime("%Y%m%d")}</tye:DocumentDate>
                                <tye:EntryDate>{datetime.date.today().strftime("%Y%m%d")}0000</tye:EntryDate>
                            </tye:Document>
                        </tye:{self.document}>""".replace("\n", "").replace("  ", "")
        else:
            return ""
        
    def __str__(self):
        return f"{self.nrotye} - {self.tipren} - {self.nrosft} - {self.importe} - {self.compag} - {self.noveda} - {self.ctacte}"
        
class Updater:
    def __init__(self, connection):
        self.connection = connection
        self.reports = self.__get_update_reports()

    def __get_update_reports(self):
        reports = self.connection.run_query("EXEC SP_CO_REND_GET_UPDATE_CORRTH", True)
        return [Notifier(*report) for report in reports]

    def get_sender(self):
        data = ""
        for report in self.reports:
            data += report.new
            if report.new != "":
                logging.info(f"""Reporte {report.nrotye} enviado con novedad {report.noveda + 1}.""")
        return data
    
    def update_reports(self):
        for report in self.reports:
            if report.get_new_validation():
                try:
                    self.connection.run_query(f"""
                                    EXEC SP_CO_REND_UPDATE_CORRTH
                                        @TIPREN = {report.tipren}
										, @NROTYE = {report.nrotye}
										, @NOVEDA = {report.noveda + 1}""", False)
                    logging.info(f"Reporte {report.nrotye} actualizado en SQL.")
                except Exception as e:
                    logging.error(f"Error al actualizar el reporte {report.nrotye}: {e}")
                    self.connection.raise_email_error(f"Error al actualizar el reporte {report.nrotye}: {e}")

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
    connection = Connection(server, base, username, password)

    api_key = os.getenv('API_KEY')
    url_tye = os.getenv('URL')
    web_service = WebService(url_tye, api_key)
    inserter = Inserter(connection, web_service)

    inserter.cashadvance_insert()
    inserter.report_insert()

    updater = Updater(connection)
    news = updater.get_sender()
    if news:
        status = web_service.send_soap_request(news)
        if status == 200:
            updater.update_reports()
        else:
            connection.raise_email_error("Error al enviar la información de novedades a Tye.")

    connection.close()

    path_app = os.getenv('PATH_APP')
    filename = os.path.join(path_app, 'sft_rend.exe')
    
    logging.info(f"Fin de la ejecución main.exe ...")
    logging.info(f"-----------------------------------")

    try: 
        script = Script(filename)
        script.run()
    except Exception as e:
        print(f"Error al ejecutar el script {filename}: {e}")

if __name__ == "__main__":
    main()
