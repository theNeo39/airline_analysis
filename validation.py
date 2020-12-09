import pyodbc


def hive_con_validate():
    con = pyodbc.connect('DSN=Hive_DSN', autocommit=True)
    cur = con.cursor()
    cur.execute('create database if not exists airline_dataset')
    con.close()


def hive_connection():
    con = pyodbc.connect('DSN=Hive_DSN', autocommit=True)
    cur = con.cursor()
    cur.execute('create database if not exists airline_dataset')
    cur.execute('use airline_dataset')
    return con, cur


def stage_tables_validate():
    con,cur=hive_connection()
    cur.execute('select count(*) from table airports_dim_stage_table limit 3')
    cur.execute('select count(*) from table carriers_dim_stage_table limit 3')
    cur.execute('select count(*) from table plane_dim_stage_table limit 3')
    cur.execute("select count(*) from table data_fact_stage_table where year='2008' limit 3;")
    con.close()