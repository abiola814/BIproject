import pandas as pd
from sqlalchemy import create_engine

# Replace 'username', 'password', 'hostname', 'port', and 'sid' with your Oracle database information
conn_str = "oracle+cx_oracle://BAOBAB:VNP1234__@172.19.8.201:1700/twtest"
engine = create_engine(conn_str)

# Replace 'your_table_name' with the actual table nametwtest
query = "SELECT * FROM BAOBAB.TLA"
df = pd.read_sql(query, engine)

print(df)

# Now 'df' is a Pandas DataFrame containing the data from the Oracle table
