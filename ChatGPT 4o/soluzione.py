import mysql.connector
from datetime import date

# Connessione al database MySQL
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="your_username",
        password="your_password",
        database="your_database"
    )

# Funzione per aggiornare il saldo e generare report
def daily_process():
    db = connect_db()
    cursor = db.cursor(dictionary=True)

    # 1. Recuperare tutti gli utenti
    cursor.execute("SELECT * FROM utenti")
    utenti = cursor.fetchall()

    for utente in utenti:
        utente_id = utente['id']
        saldo_iniziale = utente['saldo']

        # 2. Recuperare operazioni del giorno per l'utente
        cursor.execute("""
            SELECT * FROM operazioni 
            WHERE utente_id = %s AND giorno = %s
        """, (utente_id, date.today()))
        
        operazioni = cursor.fetchall()
        saldo_finale = saldo_iniziale

        # Calcolare nuovo saldo
        for operazione in operazioni:
            saldo_finale += operazione['ammontare']

        # 3. Aggiornare saldo nel database
        cursor.execute("""
            UPDATE utenti SET saldo = %s WHERE id = %s
        """, (saldo_finale, utente_id))
        db.commit()

        # 4. Generare il report
        report_filename = f"{utente_id}.txt"
        with open(report_filename, 'w') as report_file:
            report_file.write(f"Report per Utente {utente_id} - {utente['nome']}\n")
            report_file.write(f"Saldo Iniziale: {saldo_iniziale}\n")
            report_file.write("Operazioni del giorno:\n")
            for operazione in operazioni:
                report_file.write(f"- {operazione['giorno']}: {operazione['ammontare']}\n")
            report_file.write(f"Saldo Finale: {saldo_finale}\n")

    cursor.close()
    db.close()

# Esecuzione del processo giornaliero
if __name__ == "__main__":
    daily_process()
