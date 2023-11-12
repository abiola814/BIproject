# import pandas as pd
# import psycopg2

# db_params = psycopg2.connect(
#     dbname='pattest',
#     user='bespoke',
#     password='Bes@123',
#     host='196.46.20.76',
#     port='1701'
# )
# # Establish a connection to the database
# conn = psycopg2.connect(**db_params)

# sql_query = "SELECT * FROM switch.transactions;"

# # Use Pandas to read data from the database into a DataFrame
# df = pd.read_sql_query(sql_query, conn)
from pyspark.sql.functions import date_format
from datetime import datetime, timedelta

from pyspark.sql.functions import to_timestamp
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.errors.exceptions.base import PySparkException
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
import psycopg2
from pyspark.sql.window import Window

# Initialize a connection to PostgreSQL
pg_url = "postgresql://postgres:iamherenow@127.0.0.1:5432/etl_dwh_db"
engine = create_engine(pg_url)

# Fetch the last record ID from the transaction table
last_transaction_rec_id = 0  # Default value if no records exist
sql = "SELECT id FROM transactions ORDER BY id DESC LIMIT 1"
try:
    last_transaction_rec_id = pd.read_sql_query(sql, con=engine)["id"].iloc[0]
    print("Last transaction record ID: ", last_transaction_rec_id)
except Exception as e:
    print(e)


# ------ Bespoke Data Source ---------
processing_props = {
    "url": "jdbc:postgresql://172.19.8.91:1701/pattest",

    "url": "jdbc:postgresql://172.19.8.91:1701/pattest",
    "user": "bespoke",
    "password": "Bes@123",
    "driver": "org.postgresql.Driver",
}
spark1 = SparkSession.builder.appName("Bespoke ETL Script")\
    .config("spark.driver.extraClassPath", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar")\
    .config("spark.jars", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar")\
    .config("spark.sql.debug.maxToStringFields", 2000000)\
    .getOrCreate()

# Fetch the process record
conn = psycopg2.connect(
    dbname='etl_dwh_db',
    user='postgres',
    password='iamherenow',
    host='127.0.0.1',
    port='5432'
)



# fetch the last record ID from transaction table.




# Initialize Pandas DataFrame from the source PostgreSQL table

# # Define username and password
# username = 'bespoke'
# password = 'Bes%40123'

# # Define the hostname and port
# hostname = '196.46.20.76'
# port = 1701

# # Construct the PostgreSQL connection string
# switch_url = f"postgresql://{username}:Bes%40123@{hostname}:{port}/pattest"
# switch_engine = create_engine(switch_url)

table_name1 = "switch.transactions"
columns_to_select_from_ds = [
    "id",
    "institution",
    "beneficiarybank",
    "country",
    "uan",
    "currency",
    "merchantid",
    "terminalid",
    "channel",
    "transactiontype",
    "issuccessful",
    "rrn",
    "initiated",
    "beneficiaryname",
    "creditcode",
    "creditroute",
    "debitroute",
    "ussd_network",
    "debitcode",
    "superagentcode",
    "agentcode",
    "agentid",
    "subagentcommission",
    "superagentcommission",
    "depositor",
    "receipt",
    "ispaymentrequest",
    "holderid",
    "referencetrxid",
    "beneficiaryuan",
    "balance",
    "agent",
    "terminaldate",
    "mcc",
    "accesslogid",
    "productcode",
    "proccode",
    "reversalreason",
    "reversedtransactionid",
    "isreversal",
    "reversaldate",
    "actualdate",
    "requestid",
    "reversaltransactionid",
    "completionurl",
    "acquirer",
    "issuer",
    "isoffline",
    "senderaccount",
    "senderbank",
    "sendername",
    "beneficiary",
    "narration",
    "ussd_fee",
    "completed",
    "authorizationref",
    "direction",
    "merchantaccount",
    "merchantfee",
    "totalamount",
    "fee",
    "stan",
    "description",
    "statuscode"
]

try:
    #df1 = pd.read_sql_table(table_name1, con=switch_engine, columns=columns_to_select_from_ds)
    pandas_df = spark1.read.jdbc(url=processing_props['url'], table=table_name1, properties=processing_props).select(
            columns_to_select_from_ds)
except Exception as err1:
    print(err1)

    # Cast the 'datetime_column' to a format that Pandas can understand
dat_format = "yyyy-MM-dd HH:mm:ss"
# Assuming 'initiated' is the name of the datetime column in the PySpark DataFrame
pandas_df= pandas_df.withColumn('initiated', date_format(pandas_df['initiated'], dat_format))
pandas_df= pandas_df.withColumn('terminaldate', date_format(pandas_df['terminaldate'], dat_format))
pandas_df= pandas_df.withColumn('actualdate', date_format(pandas_df['actualdate'], dat_format))
pandas_df= pandas_df.withColumn('reversaldate', date_format(pandas_df['reversaldate'], dat_format))
pandas_df= pandas_df.withColumn('completed', date_format(pandas_df['completed'], dat_format))

df1 = pandas_df.toPandas()
df1 = df1.sort_values(by='initiated', ascending=True)
df1 = df1.rename(
    columns={
      
    "id": "department_table_id",
    "transactiontype": "transaction_type",
    "initiated": "transaction_time",
    "channel": "channel",
    "terminalid": "aquirer_terminal_id",
    "beneficiarybank": "beneficiary_bank",
    "beneficiaryname": "beneficiary_name",
    "country": "acquirer_country",
    "uan": "pan",
    "currency": "currency",
    "rnn": "rrn",
    "institution": "issuer_institution_id",
    "institution": "issuer_institution_name",
    "amount": "amount",
    "issuccessful": "transaction_status",
    "status": "status",
    "statuscode": "status_code",
    "description": "description",
    "stan": "stan",
    "fee": "fee",
    "totalamount": "total_amount",
    "merchantfee": "merchant_fee",
    "merchantaccount": "merchant_account",
    "direction": "direction",
    "authorizationref": "authorization_ref",
    "completed": "completed",
    "narration": "narration",
    "beneficiary": "beneficiary",
    "sendername": "sender_name",
    "senderbank": "sender_bank",
    "senderaccount": "sender_account",
    "location": "location_",
    "completionurl": "completion_url",
    "reversaltransactionid": "reversal_transaction_id",
    "requestid": "request_id",
    "actualdate": "actual_date",
    "reversaldate": "reversal_date",
    "isreversal": "is_reversal",
    "reversedtransactionid": "reversed_transaction_id",
    "reversalreason": "reversal_reason",
    "proccode": "proc_code",
    "productcode": "product_code",
    "accesslogid": "accesslog_id",
    "mcc": "mcc",
    "terminaldate": "terminal_date",
    "agent": "agent",
    "balance": "balance",
    "ip": "ip",
    "beneficiaryuan": "beneficiary_uan",
    "referencetrxid": "reference_trx_id",
    "holderid": "holder_id",
    "ispaymentrequest": "is_payment_request",
    "receipt": "receipt",
    "depositor": "depositor",
    "superagentcommission": "super_agent_commission",
    "subagentcommission": "sub_agent_commission",
    "agentid": "agent_id",
    "agentcode": "agent_code",
    "superagentcode": "super_agent_code",
    "debitcode": "debit_code",
    "creditcode": "credit_code",
    "creditroute": "credit_route",
    "issuerfee": "issuer_fee",
    "debitroute": "debit_route",
    "ussd_fee": "ussd_fee",
    "ussd_network": "ussd_network",
    "merchantid":"merchant_id",
    "isoffline":"is_offline",
}


)

# Add new columns
df1["department"] = "bespoke"
df1["account_curreny"]=df1["currency"]
df1["issuer_country"]=df1["acquirer_country"]
df1["secondary_channel"] = None
df1["acquirer_ps_name"] = None
df1["acquirer_institution_id"] = None
df1["acquirer_term_state"] = None
df1["acquirer_term_city"] = None
df1["term_owner"] = None
df1["term_entry_caps"] = None
df1["from_account"] = None
df1["to_account"] = None
df1["to_date_"] = None
df1["from_date"] = None
df1["bonus"] = None
df1["error"] = None
df1["proc_duration"] = None
df1["auth_duration"] = None
df1["transaction_number"] = None
df1["card_type"] = None
df1["curreny_orig"] = None
df1["tran_category"] = None



# Convert 'initiated' column to a datetime type
df1['transaction_time'] = pd.to_datetime(df1['transaction_time'])
column_names = df1.columns.tolist()

# Print the list of column names
print(column_names)
# Calculate the current date
current_date = datetime.now()


# Calculate the start and end dates for the previous 30 days
cur = conn.cursor()
 # Get the process record - record field.
cur.execute("select department from public.transactions where department = %s", ('bespoke', ))
process_record = cur.fetchone()


if process_record is None:
    start_date = current_date - timedelta(days=30)
    end_date = current_date

else:
    start_date = current_date - timedelta(days=1)
    end_date = current_date

# Filter the DataFrame based on the datetime range
filtered_df = df1[(df1['transaction_time'] >= start_date) & (df1['transaction_time'] <= end_date)]


print(filtered_df.count())

# res_df = df1[bespoke_last_update : BATCH_COUNT + bespoke_last_update]

# Write data to the PostgreSQL data warehouse
try:
    filtered_df.to_sql("transactions", con=engine, if_exists="append", index=False, chunksize=1000)
    print("Data transferred to the data warehouse")
except Exception as e:
    print(e)

# Confirm if the records were loaded completely
transaction_last_record = pd.read_sql_query(
    "SELECT id FROM transactions ORDER BY id DESC LIMIT 1", con=engine
)
print("Total transaction ID:", transaction_last_record["id"].iloc[0])
value_to_store_on_bespoke = 0
print(f"Value to store in process record: {value_to_store_on_bespoke}")
data_loss = len(df1) - value_to_store_on_bespoke
print(f"Remaining data on bespoke: {data_loss}")
cur.execute("insert into process_record(script_name, record) values(%s, %s)", ('bespoke', len(filtered_df)))
conn.commit()
print("Data transfer completed for bespoke")
spark1.stop()