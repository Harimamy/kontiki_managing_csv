import psycopg2


# connexion database postgres
def postgres_connect():
    config = {
        'database': "DW-Kontiki",
        'user': "postgres",
        'host': "127.0.0.1",
        'password': "postgreskontiki",
        'port': "5432"
    }
    con = psycopg2.connect(**config)
    cur = con.cursor()
    return con, cur


# cur.execute("""CREATE TABLE DATAS(
#             )""")
