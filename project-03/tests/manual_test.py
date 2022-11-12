#%%
from library.db_client import DbClient
import tests.test_sql as sql
from configparser import ConfigParser
import time
# %%

config = ConfigParser()
config.read_file(open('dwh.cfg'))

client = DbClient(config)
# %%

client.execute_query(sql.songplays_table_create_no_sort_dist)

client.execute_query(sql.songplays_table_insert_no_sort_dist)
# %%

t0 = time.time()

client.execute_query(sql.test_query_5a)

print(time.time() - t0)

t0 = time.time()
client.execute_query(sql.test_query_5b)

print(time.time() - t0)
# %%

