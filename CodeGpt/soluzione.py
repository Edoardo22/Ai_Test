import mysql.connector
import logging
from datetime import datetime
import os

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurazione del database
db_config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'banca'
}

def get_utenti(cursor):
    cursor.execute("SELECT id, nome, primo_deposito FROM utenti")
    return cursor.fetchall()

def get_operazioni(cursor, utente_id):
    cursor.execute("SELECT giorno, ammontare FROM operazioni WHERE utente_id = %s ORDER BY giorno", (utente_id,))
    return cursor.fetchall()

def update_saldo(cursor, utente_id, nuovo_saldo):
    cursor.execute("UPDATE utenti SET saldo = %s WHERE id = %s", (nuovo_saldo, utente_id))

def crea_report(utente, operazioni):
    report_filename = f"{utente['id']}.txt"
    with open(report_filename, 'w') as report_file:
        report_file.write(f"{utente['nome']}\n\n")
        for operazione in operazioni:
            data = operazione['giorno'].strftime("%d/%m/%Y")
            ammontare = f"€ {operazione['ammontare']:,.2f}"
            report_file.write(f"{data} ** {ammontare.rjust(10)}\n")
    return report_filename

def main():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        utenti = get_utenti(cursor)
        
        for utente in utenti:
            logging.info(f"Processing user: {utente['nome']} (ID: {utente['id']})")
            
            operazioni = get_operazioni(cursor, utente['id'])
            nuovo_saldo = utente['primo_deposito'] + sum(op['ammontare'] for op in operazioni)
            
            report_filename = crea_report(utente, operazioni)
            logging.info(f"Report created: {report_filename}")
            
            update_saldo(cursor, utente['id'], nuovo_saldo)
            conn.commit()
            logging.info(f"Saldo updated for user ID: {utente['id']}")
        
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
