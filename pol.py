
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id
import psycopg2

def run_bespoke_etl():
    print("--------------- RUNNING ETL FOR BESPOKE ----------------")

    # Initialize Spark session
    spark = SparkSession.builder.appName("Bespoke ETL Script")\
        .config("spark.driver.extraClassPath", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar")\
        .config("spark.jars", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar")\
        .config("spark.sql.debug.maxToStringFields", 2000000)\
        .getOrCreate()

    # Bespoke Data Source connection properties
    processing_props = {
        "url": "jdbc:postgresql://196.46.20.76:1701/pattest",
        "user": "bespoke",
        "password": "Bes@123",
        "driver": "org.postgresql.Driver",
    }

    # Connection properties to Data Warehouse
    pg_url = "jdbc:postgresql://127.0.0.1:5432/etl_dwh_db"
    connection_properties = {
        "user": "postgres",
        "password": "iamherenow",
        "driver": "org.postgresql.Driver",
        "driverLocation": "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar"
    }

    # Target table in PostgreSQL Data Warehouse
    table_name1 = "switch.transactions"
    table_name2 = "public.transaction"

    # Columns to select from data source
    columns_to_select_from_ds = [
        'id', 'institution', 'beneficiarybank', 'country',
        'uan', 'currency', 'merchantid', 'terminalid',
        'channel', 'transactiontype', 'issuccessful', 'rrn'
    ]

    # Establish a connection to the target PostgreSQL Data Warehouse
    conn = psycopg2.connect(
        dbname='etl_dwh_db',
        user='postgres',
        password='iamherenow',
        host='127.0.0.1',
        port='5432'
    )

    # Fetch the last record ID from the "transaction" table
    cur = conn.cursor()
    cur.execute("SELECT id FROM transaction ORDER BY id DESC LIMIT 1")
    last_transaction_rec = cur.fetchone()

    # Get the process record - record field
    cur.execute("SELECT * FROM process_record WHERE script_name = %s", ('bespoke',))
    process_record = cur.fetchone()

    if process_record is None:
        cur.execute("INSERT INTO process_record (script_name, record) VALUES (%s, %s)", ('bespoke', 0))
        conn.commit()
        bespoke_last_update = 0
    else:
        print(process_record)
        bespoke_last_update = process_record[1]

    if last_transaction_rec is None:
        try:
            df1 = spark.read.jdbc(url=processing_props['url'], table=table_name1, properties=processing_props).select(
                columns_to_select_from_ds)
        except Exception as err1:
            spark.stop()
            print(err1)
            return
        last_transaction_id= 0
        perform_etl(df1, conn, cur, pg_url, table_name2, connection_properties, spark, bespoke_last_update,last_transaction_id)

    else:
        # Read data from PostgreSQL into a DataFrame
        try:
            df1 = spark.read.jdbc(url=processing_props['url'], table=table_name1, properties=processing_props).select(
                columns_to_select_from_ds)
        except Exception as err1:
            spark.stop()
            print(err1)
            return
        last_transaction_id= last_transaction_rec[0]
        perform_etl(df1, conn, cur, pg_url, table_name2, connection_properties, spark, bespoke_last_update,last_transaction_id)

    # Stop the Spark session
    spark.stop()

def perform_etl(df, conn, cur, pg_url, table_name, connection_properties, spark, bespoke_last_update,last_transaction_id):
    # Order the data frame by 'id'
    ordered_df = df.orderBy('id')

    # Rename dataframe columns to match the database table column names
    ds_dataframe = df.toDF(
        "department_table_id", "issuer_institution_id", "issuer_institution_name",
        "issuer_country_code", "pan", "account_currency",
        "merchant_id", "terminal_id", "channel", "transaction_type",
        "transaction_status", "rrn"
    )

    # Add new columns to the dataframe
    ds_dataframe_with_processing = ds_dataframe.withColumn("department", lit("bespoke")) \
        .withColumn("status_code", lit(None).cast('string')) \
        .withColumn("id", monotonically_increasing_id() + (last_transaction_id + 1)) \
        .withColumn("settlement_status", lit(None).cast('string')) \
        .withColumn("transaction_date", lit(None).cast('string')) \
        .withColumn("merchant_name", lit(None).cast('string')) \
        .withColumn("terminal_owner", lit(None).cast('string')) \
        .withColumn("charged_back_or_disputed", lit(None).cast('string')) \
        .withColumn("domestic_or_foreign", lit(None).cast('string')) \
        .withColumn("row_num", lit(None).cast('string')) \
        .withColumn("acquirer_institution_id", lit(None).cast('string')) \
        .withColumn("acquirer_institution_name", lit(None).cast('string')) \
        .withColumn("acquirer_country_code", lit(None).cast('string')) \
        .withColumn("channel_secondary", lit(None).cast('string'))

    ds_dataframe_with_processing.show()

    # Order the dataframe
    ordered_df = ds_dataframe_with_processing.orderBy('id')
    total_ordered_df_count = ordered_df.count()

    # Write to the Data Warehouse if the record is 0.
    print("---------- Writing Data Frame into Data Warehouse ----------")
    print("appending...")
    ds_dataframe_with_processing.write.jdbc(url=pg_url, table=table_name, mode="append", properties=connection_properties)

    print("------------- Loaded data -------------")

    # Confirm if records were loaded completely
    cur.execute("SELECT id FROM transaction ORDER BY id DESC LIMIT 1")
    transaction_last_record = cur.fetchone()

    print('------- Data transfer completed for bespoke --------')

if __name__ == "__main__":
    print("jj")
    run_bespoke_etl()
