# This is a sample Python script.

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas
import mariadb
import sys
import click
from datetime import datetime
from getpass import getpass


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', print_end="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + ' ' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    # Print New Line on Complete
    if iteration == total:
        print()


class Album:
    def __init__(self, bcode, art, tit, lab, sup, pr):
        self.barcode = str(bcode)
        self.artista = art
        self.titolo = tit
        self.label = lab
        self.supporto = sup.lower()
        ps = pr - pr * 0.05
        self.prezzo = str(format(ps + (ps * 0.22) + ((ps + (ps * 0.22)) * 0.25), '.2f'))

    def __str__(self):
        return "Barcode:" + self.barcode + "\n" + \
               "Artista:" + self.artista + "\n" + \
               "Titolo:" + self.titolo + "\n" + \
               "Label:" + self.label + "\n" + \
               "Supporto:" + self.supporto + "\n" + \
               "Prezzo:" + self.prezzo + "\n\n"


@click.command()
@click.option("-u", "--U", "user", help="User del database")
@click.option("-f", "--file", "ifile", help="File da caricare")
@click.option("-V", "--vendita", "tipo", flag_value="vendita", help="Carico il file nella tabella 'ordini_online'")
@click.option("-A", "--acquisti", "tipo", flag_value="acquisti", help="Carico il file nella tabella 'cd' ")
@click.option("-I", "--inventario", "tipo", flag_value="inventario", help="Carico il file nella tabella 'online_shop'")
def main(user, ifile, tipo):
    global conn
    try:
        if tipo is None:
            click.secho("***********************************************************************\n"
                        "* Attenzione! Heavy Loader ha bisogno di più parametri per funzionare *\n"
                        "* E' necessario scegliere una modalità di esecuzione: -V, -A, -I      *\n"   
                        "* usare l'opzione --help per leggere il funzionamento dello script.   *\n"
                        "***********************************************************************", fg='red')
            sys.exit(1)

        conn = None
        now = datetime.now()
        click.secho("********************************************************************************* \n"
                    "* " + now.strftime("%H:%M:%S") + " Hello! Heavy Loader will load the data contained in file: "
                    + ifile + " *\n" +
                    "*********************************************************************************",
                    fg='green', bold=True)
        click.secho("* Authorization needed: please insert password", fg='red', blink=True)

        password = getpass("* Password:")
        click.secho("* Connecting to database:")
        conn = mariadb.connect(
            user=user,
            password=password,
            host="192.168.1.24",
            port=3306,
            database="magazzino"
        )
        now = datetime.now()
        click.secho("* " + now.strftime("%H:%M:%S") + " Connection OK", fg='green')

        cur = conn.cursor()
        df = pandas.read_csv(ifile)
        now = datetime.now()
        click.secho("* " + now.strftime("%H:%M:%S") + " File parsed", fg='green')

        header = list(df)

        if tipo == "acquisti" and "barcode" in header:
            df['barcode'] = df['barcode'].apply(lambda x: '{0:0>13}'.format(x))
            rows = df.shape[0]
            now = datetime.now()
            click.secho("* " + now.strftime("%H:%M:%S") + " Loading data")
            print_progress_bar(0, rows, prefix='* Progress:', suffix='Complete', length=50)

            for i, r in df.iterrows():
                a = Album(r["barcode"], r["artista"], r["titolo"], r["etichetta"],
                          r["supporto"], float(r["prezzo"].replace(",", ".")))
                cur.execute("INSERT IGNORE INTO cd (barcode, type, singer, title, label, price) "
                            "VALUES(?, ?, ?, ?, ?, ?)",
                            (a.barcode, a.supporto, a.artista, a.titolo, a.label, a.prezzo))
                cur.execute("INSERT IGNORE INTO acqui (barcode, type, singer, title, label, price) "
                            "VALUES(?, ?, ?, ?, ?, ?)",
                            (a.barcode, a.supporto, a.artista, a.titolo, a.label, a.prezzo))

                print_progress_bar(i + 1, rows, prefix='* Loading Progress:', suffix='Complete', length=50)

        elif tipo == "vendita":
            print("carico ordini online in 'online_orders'")

        elif tipo == "inventario" and "listing_id" in header:
            rows = df.shape[0]
            now = datetime.now()
            click.secho("* " + now.strftime("%H:%M:%S") + " Loading data")
            print_progress_bar(0, rows, prefix='* Progress:', suffix='Complete', length=50)
            cur.execute("delete from online_store")
            for i, r in df.iterrows():
                if r["status"] == "For Sale":
                    cur.execute("INSERT IGNORE INTO online_store (listing_id, artist, title, label, catno,"
                                "format, release_id, price, listed, comments, media_condition, sleeve_condition,"
                                "weight, location) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ",
                                (r["listing_id"], r["artist"], r["title"], r["label"], r["catno"], r["format"],
                                r["release_id"], r["price"], r["listed"], r["comments"], r["media_condition"],
                                r["sleeve_condition"], r["weight"], r["location"]))
                    print_progress_bar(i + 1, rows, prefix='* Loading Progress:', suffix='Complete', length=50)
        else:
            print("* Il file scelto non contiene i dati richiesti da questa modalità")

        now = datetime.now()
        conn.close()
        click.secho("* " + now.strftime("%H:%M:%S") + " Data loaded correctly!", fg='green')
        click.secho("* Program ends \n* Bye! "
                    "\n*********************************************************************************")

    except mariadb.Error as e:
        now = datetime.now()
        click.secho("* " + now.strftime("%H:%M:%S") + " Aborting! Database Error: " + str(e), fg='red')
        click.secho("*********************************************************************************")
        sys.exit(1)
    except OSError as e:
        now = datetime.now()
        click.secho("* " + now.strftime("%H:%M:%S") + " Aborting! File error: " + str(e), fg='red')
        conn.close()
        click.secho("*********************************************************************************")
        sys.exit(1)
    except KeyboardInterrupt:
        now = datetime.now()
        click.secho("\n* " + now.strftime("%H:%M:%S") + " Aborting! Kayboard interrupt requested", fg='red')
        if conn is not None:
            conn.close()
        click.secho("*********************************************************************************")
        sys.exit(1)


if __name__ == '__main__':
    main()
