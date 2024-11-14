# Per realizzare il progetto descritto nel README, possiamo sviluppare uno script Python che soddisfi i requisiti specificati. Di seguito ti fornisco una guida passo-passo per implementare il progetto:

# Step 1: Impostazione dell'ambiente
# Crea un file requirements.txt per gestire le dipendenze. Potresti aver bisogno di librerie come pymysql per connetterti al database MySQL e logging per gestire i log. Contenuto del file requirements.txt:
# pymysql

# Costruisci e avvia il container Docker per avere accesso al database:
# docker build -t ch_py_mysql .
# docker run -it --rm -p 3306:3306 --name ch_py_mysql ch_py_mysql

# Step 2: Connessione al Database
# Scrivi una funzione per connetterti al database MySQL:

import pymysql

def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='your_username',
        password='your_password',
        database='your_database',
        port=3306
    )

# Step 3: Creazione del Report e Aggiornamento del Saldo
# Implementa la logica per generare i report e aggiornare il saldo:

import logging
from decimal import Decimal

logging.basicConfig(level=logging.INFO)

def process_user_data():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Recupera tutti gli utenti
            cursor.execute("SELECT id, nome, primo_deposito FROM utenti")
            utenti = cursor.fetchall()

            for utente in utenti:
                user_id, nome, primo_deposito = utente
                logging.info(f"Processing user: {nome} (ID: {user_id})")

                # Recupera le operazioni per l'utente
                cursor.execute("SELECT giorno, ammontare FROM operazioni WHERE utente_id = %s ORDER BY giorno", (user_id,))
                operazioni = cursor.fetchall()

                # Crea il report
                report_lines = [nome, ""]
                saldo = primo_deposito

                for operazione in operazioni:
                    giorno, ammontare = operazione
                    saldo += ammontare
                    report_lines.append(f"{giorno.strftime('%d/%m/%Y')} ** € {ammontare:10.2f}")

                # Se non ci sono operazioni, il report avrà solo il nome
                if not operazioni:
                    report_lines = [nome]

                # Scrivi il report su file
                with open(f"{user_id}.txt", "w") as report_file:
                    report_file.write("\n".join(report_lines))

                # Aggiorna il saldo nel database
                cursor.execute("UPDATE utenti SET saldo = %s WHERE id = %s", (saldo, user_id))
                connection.commit()

                logging.info(f"Report created and balance updated for user: {nome} (ID: {user_id})")

    finally:
        connection.close()

if __name__ == "__main__":
    process_user_data()

# Step 4: Esegui lo Script
# Assicurati di avere il database in esecuzione e poi esegui lo script Python:

# python your_script_name.py
# Note Finali
# Assicurati di sostituire your_username, your_password, e your_database con le credenziali corrette.
# Il logging ti aiuterà a monitorare lo stato del processo e a identificare eventuali errori.
# La connessione al database e le operazioni di scrittura devono essere gestite in modo atomico per garantire la coerenza dei dati.