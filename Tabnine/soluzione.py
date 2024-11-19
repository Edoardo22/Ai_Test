import mysql.connector
import logging
from datetime import date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root',
            database='mydatabase'
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to the database: {err}")
        return None

def get_utenti(cursor):
    cursor.execute("SELECT id, nome, primo_deposito, saldo FROM utenti")
    return cursor.fetchall()

def get_operazioni(cursor, utente_id):
    cursor.execute("""
        SELECT giorno, ammontare
        FROM operazioni
        WHERE utente_id = %s
        ORDER BY giorno ASC
    """, (utente_id,))
    return cursor.fetchall()

def update_saldo(cursor, utente_id, nuovo_saldo):
    cursor.execute("""
        UPDATE utenti
        SET saldo = %s
        WHERE id = %s
    """, (nuovo_saldo, utente_id))

def crea_report(utente, operazioni):
    report_lines = [f"Report per Utente {utente['id']} - {utente['nome']}"]
    report_lines.append(f"Saldo Iniziale: {utente['primo_deposito']}")
    report_lines.append("Operazioni:")
    for op in operazioni:
        report_lines.append(f"- {op['giorno']}: {op['ammontare']}")
    report_lines.append(f"Saldo Finale: {utente['saldo']}")
    
    report_filename = f"{utente['id']}.txt"
    with open(report_filename, 'w', encoding='utf-8') as report_file:
        report_file.write('\n'.join(report_lines))
    
    return report_filename

def main():
    conn = connect_db()
    if conn is None:
        return

    cursor = conn.cursor(dictionary=True)
    
    try:
        utenti = get_utenti(cursor)
        
        for utente in utenti:
            logging.info(f"Processing user: {utente['nome']} (ID: {utente['id']})")
            
            operazioni = get_operazioni(cursor, utente['id'])
            nuovo_saldo = utente['primo_deposito'] + sum(op['ammontare'] for op in operazioni)
            
            update_saldo(cursor, utente['id'], nuovo_saldo)
            conn.commit()
            logging.info(f"Saldo updated for user ID: {utente['id']}")
            
            report_filename = crea_report(utente, operazioni)
            logging.info(f"Report created: {report_filename}")
        
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()