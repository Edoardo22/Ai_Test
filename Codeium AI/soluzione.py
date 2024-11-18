import mysql.connector
from datetime import date

# Connessione al database MySQL
conn = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='root',
    password='root',
    database='mydatabase'
)
cursor = conn.cursor(dictionary=True)

# Recupera tutti gli utenti
cursor.execute("SELECT id, nome, primo_deposito FROM utenti")
utenti = cursor.fetchall()

# Processa ogni utente
for utente in utenti:
    user_id = utente['id']
    nome = utente['nome']
    primo_deposito = utente['primo_deposito']

    # Recupera le operazioni dell'utente
    cursor.execute("""
        SELECT giorno, ammontare
        FROM operazioni
        WHERE utente_id = %s
        ORDER BY giorno ASC
    """, (user_id,))
    operazioni = cursor.fetchall()

    # Calcola il nuovo saldo
    totale_operazioni = sum(op['ammontare'] for op in operazioni)
    nuovo_saldo = primo_deposito + totale_operazioni

    # Aggiorna il saldo nella tabella utenti
    cursor.execute("""
        UPDATE utenti
        SET saldo = %s
        WHERE id = %s
    """, (nuovo_saldo, user_id))
    conn.commit()

    # Crea il contenuto del report
    report_lines = [nome]
    if operazioni:
        report_lines.append('')
        for op in operazioni:
            data_formattata = op['giorno'].strftime('%d/%m/%Y')
            ammontare_formattato = "€{:9,.2f}".format(op['ammontare'])
            report_lines.append(f"{data_formattata} ** {ammontare_formattato}")

    # Scrive il report su file
    report_content = '\n'.join(report_lines)
    with open(f"{user_id}.txt", 'w', encoding='utf-8') as report_file:
        report_file.write(report_content)

# Chiudi la connessione al database
cursor.close()
conn.close()