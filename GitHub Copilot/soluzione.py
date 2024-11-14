# pip install mysql-connector-python

import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="banca"
    )

import os
import logging
from database import get_connection

logging.basicConfig(level=logging.INFO)

def fetch_users(cursor):
    cursor.execute("SELECT id, nome, primo_deposito FROM utenti")
    return cursor.fetchall()

def fetch_operations(cursor, user_id):
    cursor.execute("SELECT giorno, ammontare FROM operazioni WHERE utente_id = %s ORDER BY giorno", (user_id,))
    return cursor.fetchall()

def update_balance(cursor, user_id, new_balance):
    cursor.execute("UPDATE utenti SET saldo = %s WHERE id = %s", (new_balance, user_id))

def create_report(user, operations):
    report_path = f"{user['id']}.txt"
    with open(report_path, 'w') as file:
        file.write(f"{user['nome']}\n\n")
        for op in operations:
            file.write(f"{op['giorno'].strftime('%d/%m/%Y')} ** € {op['ammontare']:10.2f}\n")
    logging.info(f"Report created: {report_path}")

def main():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    users = fetch_users(cursor)
    for user in users:
        operations = fetch_operations(cursor, user['id'])
        create_report(user, operations)
        
        new_balance = user['primo_deposito'] + sum(op['ammontare'] for op in operations)
        update_balance(cursor, user['id'], new_balance)
        conn.commit()
        logging.info(f"Balance updated for user {user['id']}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()