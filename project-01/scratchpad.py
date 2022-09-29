#%%
import create_tables

create_tables.main()


# %%
import psycopg2

conn = psycopg2.connect("host=127.0.0.1 port=6543 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

cur.execute("select * from artists;")
cur.fetchall()

# %%
