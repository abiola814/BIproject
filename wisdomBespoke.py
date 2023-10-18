print("--------------- RUNNING ETL FOR BESPOKE ----------------")
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.errors.exceptions.base import PySparkException
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
import psycopg2
from pyspark.sql.window import Window



# Initialize Spark session
spark1 = SparkSession.builder.appName("Bespoke ETL Script")\
    .config("spark.driver.extraClassPath", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar")\
    .config("spark.jars", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar")\
    .config("spark.sql.debug.maxToStringFields", 2000000)\
    .getOrCreate()

# ------ Bespoke Data Source ---------
processing_props = {
    "url": "jdbc:postgresql://196.46.20.76:1701/pattest",

    "url": "jdbc:postgresql://196.46.20.76:1701/pattest",
    "user": "bespoke",
    "password": "Bes@123",
    "driver": "org.postgresql.Driver",
}


# Connection properties to Data Ware House
pg_url = "jdbc:postgresql://127.0.0.1:5432/etl_dwh_db"
connection_properties = {
    "user": "postgres",
    "password": "iamherenow",
    "driver": "org.postgresql.Driver",
    "driverLocation": "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar"
}

# Target table in PostgreSQL Data warehouse
table_name1 = "switch.transactions"

# Columns to select from data source
columns_to_select_from_ds = [
    'id', 'institution', 'beneficiarybank', 'country',
    'uan', 'currency', 'merchantid', 'terminalid',
    'channel', 'transactiontype', 'issuccessful', 'rrn'
]

conn = psycopg2.connect(
    dbname='etl_dwh_db',
    user='postgres',
    password='iamherenow',
    host='127.0.0.1',
    port='5432'
)



# fetch the last record ID from transaction table.
cur = conn.cursor()
cur.execute("SELECT id FROM transaction ORDER BY id DESC LIMIT 1")
last_transaction_rec = cur.fetchone()
last_transaction_rec_id = cur.fetchone()
print("last ttansaction rec id  ",last_transaction_rec_id)
 # Get the process record - record field.
cur.execute("select * from process_record where script_name = %s", ('bespoke', ))
process_record = cur.fetchone()

if process_record is None:
    cur.execute("insert into process_record (script_name, record) values (%s, %s)", ('bespoke', 0))
    conn.commit()
    bespoke_last_update = 0
else:
    print(process_record)
    bespoke_last_update = process_record[2]
if last_transaction_rec is None:
    try:
        df1 = spark1.read.jdbc(url=processing_props['url'], table=table_name1, properties=processing_props).select(
            columns_to_select_from_ds)

    except(Exception,) as err1:
        spark1.stop()
        print(err1)

    # Order the data frame by id
    ordered_df = df1.orderBy('id')

    # ------ replace all dataframe column name to match the database table column names -------
    ds_dataframe = df1.withColumnsRenamed(
        colsMap={
            "id": "department_table_id", "institution": "issuer_institution_id", "beneficiarybank": "issuer_institution_name",
            "country": "issuer_country_code", "uan": "pan", "currency": "account_currency",
            "merchantid": "merchant_id", "terminalid": "terminal_id",
            "channel": "channel", "transactiontype": "transaction_type",
            "issuccessful": "transaction_status", "totalamount": "amount", "rrn": "rrn"
           }
     )


   # Add a new column 'department'
    ds_dataframe_with_processing = ds_dataframe.withColumns({"department": lit("bespoke"),
                                            "status_code": F.lit(None).cast('string'), "id": monotonically_increasing_id() + (last_transaction_rec_id + 1),
                                            "settlement_status": F.lit(None).cast('string'), "transaction_date": F.lit(None).cast('string'),
                                            "merchant_name": F.lit(None).cast('string'), "terminal_owner": F.lit(None).cast('string'),
                                            "charged_back_or_disputed": F.lit(None).cast('string'), "domestic_or_foreign": F.lit(None).cast('string'),
                                            "row_num": F.lit(None).cast('string'), "acquirer_institution_id": F.lit(None).cast('string'), "acquirer_institution_name": F.lit(None).cast('string'),
                                            "acquirer_country_code": F.lit(None).cast('string'), "channel_secondary": F.lit(None).cast('string')
                                       })

    ds_dataframe_with_processing.show()

    # Order the dataframe
    ordered_df = ds_dataframe_with_processing.orderBy('id')
    total_ordered_df_count = ordered_df.count()

    # Ignoring Data Loss

    # Write to dwh if the record is 0.
    print("---------- Writing Data Frame into Dataware house ----------")
    print("appending...")
    ds_dataframe_with_processing.write.jdbc(url=pg_url, table='public.transaction', mode="append", properties=connection_properties)

    print("------------- Loaded data -------------")

    # Confirm if record was loaded completely
    cur.execute("SELECT id FROM transaction ORDER BY id DESC LIMIT 1")
    transaction_last_record = cur.fetchone()


    print('------- Data transfer completed for bespoke --------')


else:
# Read data from PostgreSQL into a DataFrame
    try:
        df1 = spark1.read.jdbc(url=processing_props['url'], table=table_name1, properties=processing_props).select(
            columns_to_select_from_ds)

    except(Exception,) as err1:
        spark1.stop()
        print(err1)

    # Order the data frame by id
    ordered_df = df1.orderBy('id')

    # ------ replace all dataframe column name to match the database table column names -------
    ds_dataframe = df1.withColumnsRenamed(
        colsMap={
            "id": "department_table_id", "institution": "issuer_institution_id", "beneficiarybank": "issuer_institution_name",
            "country": "issuer_country_code", "uan": "pan", "currency": "account_currency",
            "merchantid": "merchant_id", "terminalid": "terminal_id",
            "channel": "channel", "transactiontype": "transaction_type",
            "issuccessful": "transaction_status", "totalamount": "amount", "rrn": "rrn"
           }
     )


   # Add a new column 'department'
    ds_dataframe_with_processing = ds_dataframe.withColumns({"department": lit("bespoke"),
                                            "status_code": F.lit(None).cast('string'), "id": monotonically_increasing_id() + (last_transaction_rec_id + 1),
                                            "settlement_status": F.lit(None).cast('string'), "transaction_date": F.lit(None).cast('string'),
                                            "merchant_name": F.lit(None).cast('string'), "terminal_owner": F.lit(None).cast('string'),
                                            "charged_back_or_disputed": F.lit(None).cast('string'), "domestic_or_foreign": F.lit(None).cast('string'),
                                            "row_num": F.lit(None).cast('string'), "acquirer_institution_id": F.lit(None).cast('string'), "acquirer_institution_name": F.lit(None).cast('string'),
                                            "acquirer_country_code": F.lit(None).cast('string'), "channel_secondary": F.lit(None).cast('string')
                                       })

    ds_dataframe_with_processing.show()

    # Order the dataframe
    ordered_df = ds_dataframe_with_processing.orderBy('id')
    total_ordered_df_count = ordered_df.count()

    # Ignoring Data Loss

    # Write to dwh if the record is 0.
    print("---------- Writing Data Frame into Dataware house ----------")
    print("appending...")
    ds_dataframe_with_processing.write.jdbc(url=pg_url, table='public.transaction', mode="append", properties=connection_properties)

    print("------------- Loaded data -------------")

    # Confirm if record was loaded completely
    cur.execute("SELECT id FROM transaction ORDER BY id DESC LIMIT 1")
    transaction_last_record = cur.fetchone()

    print("total transaction id",transaction_last_record[0])
    value_to_store_on_bespoke = int(bespoke_last_update)  + (int(transaction_last_record[0]-last_transaction_rec_id)) 
    print(f"value to be store on process record is {value_to_store_on_bespoke} ")
    print( "total dataframe on ds_dataframe_with_processing  ",ds_dataframe_with_processing.count())
    data_loss = int(ds_dataframe_with_processing.count()) - value_to_store_on_bespoke
    print( f" value of the loss on bespoke {data_loss}")

    print('------- Data transfer completed for bespoke --------')

spark1.stop()