from pyspark.sql.functions import date_format
from datetime import datetime, timedelta

from pyspark.sql.functions import to_timestamp
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
from pyspark.errors.exceptions.base import PySparkException
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
import psycopg2
from pyspark.sql.window import Window

#global variables
table_name="switch.holders"
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
# postgres 
pg_url = "postgresql://postgres:iamherenow@127.0.0.1:5432/etl_dwh_db"
engine = create_engine(pg_url)

column_from_db =my_list = [
    "phone",
    "email",
    "firstname",
    "lastname",
    "middlename",
    "address",
    "city",
    "state",
    "nationality",
    "sex",
    "marriage",
    "profession",
    "occupation",
    "bvn",
    "birthday",
    "ispremium",
    "premiumactivated",
    "accountid",
    "created",
    "isagent",
    "isverified2",
    "isverified1",
    "nin"
]

def get_last_id():
    # Fetch the last record index on holdertag
    last_rec_id = 0  # Default value if no records exist
    sql = "SELECT id FROM holdertags ORDER BY id DESC LIMIT 1"
    try:
        last_rec_id = pd.read_sql_query(sql, con=engine)["id"].iloc[0]
        print("Last transaction record ID: ", last_rec_id)
    except Exception as e:
        print(e)
    return last_rec_id
cur = conn.cursor()
 # Get the process record - record field.
cur.execute("select department from public.holdertags where department = %s", ('holders', ))
process_record = cur.fetchone()

try:
    #df1 = pd.read_sql_table(table_name1, con=switch_engine, columns=columns_to_select_from_ds)
    pandas_dfs = spark1.read.jdbc(url=processing_props['url'], table=table_name, properties=processing_props).select(
            column_from_db)
    # Calculate the date of a day before today
    current_date = datetime.now()

    start_date = None
    end_date = None

    if process_record is None:
        start_date = current_date - timedelta(days=30) # from the past e.g: last month
        end_date = current_date - timedelta(days=1) # most recent
    else:
        # This time filters needs to be properly reviewed.
        start_date = current_date - timedelta(days=1)
        end_date = current_date

    print(start_date, end_date)

    # Filter data from a day before today to last month
    pandas_df = pandas_dfs.filter((col('created') >= start_date) & (col('created') <= end_date)) # e.g => from 2nd
    pandas_df.show(200)

except Exception as err1:
    print(err1)


print(pandas_df.show())
    # Cast the 'datetime_column' to a format that Pandas can understand
dat_format = "yyyy-MM-dd HH:mm:ss"
# Assuming 'initiated' is the name of the datetime column in the PySpark DataFrame
pandas_df= pandas_df.withColumn('premiumactivated', date_format(pandas_df['premiumactivated'], dat_format))
pandas_df= pandas_df.withColumn('created', date_format(pandas_df['created'], dat_format))
pandas_df= pandas_df.withColumn("birthday", col("birthday").cast('string'))
df1 = pandas_df.toPandas()
df1 = df1.sort_values(by='created', ascending=True)
df1["department"] = "holder"
df1["schema_type"] = "holder"

items = [ 'holderid', 'isactive', 'isprepaid', 'prepaidaccountid', 'isdebit', 'bankaccount', 'pin', 'pinstreak', 'status', 'created', 'branch', 'onlineprofile', 'offlineprofile', 'institution', 'agentid', 'recurringsubscriptionfee', 'tagstatus', 'activationdate', 'lastpaymentdate', 'nextpaymentdate', 'subtype', 'subscriptiontransactionid', 'activationtransactionid', 'nickname', 'verificationaccesstime', 'verificationcomment', 'selectcount', 'verifier', 'isverified', 'agentcode', 'pendforadditionalaccount', 'isdefault', 'channel', 'recomputed']

# Loop through the items and set them to None in the DataFrame
for item in items:
    df1[item] = None

column_names = df1.columns.tolist()

# Print the list of column names
print(column_names)
# Calculate the current date
current_date = datetime.now()


# Calculate the start and end dates for the previous 30 days



# if process_record is None:
#     start_date = current_date - timedelta(days=230)
#     end_date = current_date
#     print(start_date,end_date)

# else:
#     start_date = current_date - timedelta(days=1)
#     end_date = current_date



# Filter the DataFrame based on the datetime range


print(df1.count())
print(df1.head())

try:
    df1.to_sql("holdertags", con=engine, if_exists="append", index=False, chunksize=1000)
    print("Data transferred to the data warehouse")
except Exception as e:
    print(e)

# Confirm if the records were loaded completely
last_record = pd.read_sql_query(
    "SELECT id FROM holdertags ORDER BY id DESC LIMIT 1", con=engine
)
print("Total holdertags ID:", last_record["id"].iloc[0])
