# #%%
# import time
# import pandas as pd
# import oracledb

# # Fill these in
# USER = "COREAI_USER"          # or ADMIN, etc.
# PWD  = "ChangeMe#today1"
# # WALLET_DIR = r"""C:/Users/Felicia.williams/Downloads/wallet"""

# # Direct connection descriptor (TNS string)
# DSN = (
#     "(description="
#     "(retry_count=20)(retry_delay=3)"
#     "(address=(protocol=tcps)(port=1522)(host=ykc6u08c.adb.us-phoenix-1.oraclecloud.com))"
#     "(connect_data=(service_name=g5131843e34dc90_adbcoreai_high.adb.oraclecloud.com))"
#     "(security=(ssl_server_dn_match=no)))"
# )
# # If your client bin isn't on PATH, add lib_dir:
# # oracledb.init_oracle_client(lib_dir=r"C:\Oracle64\12.2.0.1\bin", config_dir=WALLET_DIR)
# # If it is on PATH (looks like it is), just:
# # oracledb.init_oracle_client(config_dir=WALLET_DIR)
# # adbcoreai_high = (description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=ykc6u08c.adb.us-phoenix-1.oraclecloud.com))(connect_data=(service_name=g5131843e34dc90_adbcoreai_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=no)))
# t0 = time.perf_counter()
# conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)  # uses tnsnames.ora + wallet in WALLET_DIR
# print(f"Connected in {time.perf_counter()-t0:.2f}s")

# # Optional: pin schema so you can omit owner prefixes
# # with conn.cursor() as c: c.execute("alter session set current_schema=COREAI_USER")

# df = pd.read_sql("""
#     select /*+ first_rows(20) */ *
#     from COREAI_USER.BILLED_PAID_ITEMS
#     fetch first 20 rows only
# """, conn)
# print(df.head())
# conn.close()

# %%
import time
import pandas as pd
import oracledb
from tqdm import tqdm

# Fill these in
USER = "COREAI_USER"
PWD = "ChangeMe#today1"

# Direct connection descriptor (TNS string)
DSN = (
    "(description="
    "(retry_count=20)(retry_delay=3)"
    "(address=(protocol=tcps)(port=1522)(host=ykc6u08c.adb.us-phoenix-1.oraclecloud.com))"
    "(connect_data=(service_name=g5131843e34dc90_adbcoreai_high.adb.oraclecloud.com))"
    "(security=(ssl_server_dn_match=no)))"
)

# Step 1: Connect to the database
print("Connecting to Oracle ADB...")
t0 = time.perf_counter()
for _ in tqdm(range(1), desc="Connecting"):
    conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)
t1 = time.perf_counter()
print(f"✅ Connected in {t1 - t0:.2f} seconds.\n")

# Step 2: Query the data
print("Querying data from BILLED_PAID_ITEMS...")
t2 = time.perf_counter()
for _ in tqdm(range(1), desc="Querying"):
    df = pd.read_sql("""
        select /*+ first_rows(1) */ *
        from COREAI_USER.BILLED_PAID_ITEMS
        fetch first 1 rows only
    """, conn)
t3 = time.perf_counter()
print(f"✅ Query completed in {t3 - t2:.2f} seconds.\n")

# Step 3: Display results
print("Preview of results:")
print(df.head())

# Step 4: Close connection
conn.close()
print("🔒 Connection closed.")
