# Test - Python + MySQL

## Scenario

Il nostro cliente è un istituto bancario, il quale possiede una fonte dati che rappresenta lo stato dei conti dei suoi correntisti.

La fonte dati è composta da due tabelle, **utenti**:

| Campo | Tipo | Note |
|---|---|---|
|id|int|Chiave primaria|
|nome|varchar(255)|Nome del cliente|
|primo_deposito|decimal(13, 2)|Primo versamento|
|saldo|decimal(13, 2)|Saldo aggiornato

ed **operazioni**:
| Campo | Tipo | Note |
|---|---|---|
|id|int|Chiave primaria|
|utente_id|int|Riferimento a _utenti_|
|giorno|date|Data operazione|
|ammontare|decimal(13, 2)|Ammontare operazione

Il cliente ci chiede che, ogni giorno, un processo automatico legga la fonte dati e, **per ciascun utente**, esegua queste operazioni:

- **crei un report** testuale nominandolo "_[id].txt_" (dove "id" è l'id univoco dell'utente analizzato)
- **scriva** dentro ciascun file di report alcune informazioni definite
- **aggiorni** la fonte dati: il campo "_saldo_" della tabella "_utenti_" dovrà essere uguale alla somma del campo "_ammontare_" della tabella "_operazioni_" più il campo "_primo_deposito_" della tabella "_utente_" (in sostanza lo stato attuale del saldo di ciascun utente, che consideri tutte le operazioni da lui fatte)

---

## Specifica del file di Report

- il report di ogni utente dovrà avere questa struttura:
  - riga 1: nome del cliente
  - riga 2: vuota
  - dalla riga 3: data operazione in formato GG/MM/AAAA, seguita da _" ** "_ (uno spazio, due asterischi, uno spazio), seguito dall'ammontare allineato a destra, formattato con simbolo € e due decimali dopo la virgola. Le operazioni dovranno essere ordinate per data crescente
- se un utente non ha effettuato operazioni, creare un file di report con la sola riga 1

### Esempio di Report inviato dal cliente

```
Eren Jaeger

01/01/2020 ** €   523,20
02/01/2020 ** €  1785,85
03/01/2020 ** €  -125,10
04/01/2020 ** € -1875,96
```

---

## Requisiti tecnici dello Script

- deve essere scritto in **python 3.6 o superiore**
- deve **produrre un log** a video che permetta di capire a che punto è l'elaborazione, e quali errori eventualmente si sono verificati.
- deve assicurare che la scrittura del il file di report e l'aggiornamento del saldo sulla fonte dati siano "**atomiche**" per ogni correntista: il cliente considererà la mancanza del file di report come un'anomalia da poter intercettare tempestivamente. Se il report manca, la fonte dati rimarrà aggiornata all'ultima esecuzione valida del processo (il cliente è conscio di questo) ma se il report è presente, il cliente considera come totalmente affidabile il suo contenuto così come il saldo sulla fonte dati.
- deve tenere conto che il processo potrebbe dover analizzare decine di migliaia di righe, ma il cliente ha bisogno che **la fonte dati sia impegnata il meno possibile**
- deve essere accompagnato da un file **"requirements"** se si usano librerie di terze parti

---

## Indicazioni dell'ambiente di sviluppo

L'ambiente sfrutta un container Docker che possiede un dump aggiornato della fonte dati del cliente. L'immagine del container può essere creata con il comando:

```sh
docker build -t ch_py_mysql .
```

L'avvio del container può essere fatto con il comando:
```sh
docker run -it --rm -p 3306:3306 --name ch_py_mysql ch_py_mysql
```

A quel punto il database sarà contattabile in localhost sulla port 3306