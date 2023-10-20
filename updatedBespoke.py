import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id
import time

# Define the PostgreSQL Data Warehouse URL and Connection Properties
pg_url = "jdbc:postgresql://127.0.0.1:5432/etl_dwh_db"
connection_properties = {
    "user": "postgres",
    "password": "iamherenow",
    "driver": "org.postgresql.Driver",
    "driverLocation": "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar"
}

def get_last_transaction_id(conn):
    cur = conn.cursor()
    cur.execute("SELECT id FROM transaction ORDER BY id DESC LIMIT 1")
    last_transaction_rec_id = cur.fetchone()[0]
    cur.close()
    return last_transaction_rec_id

def create_spark_session():
    spark1 = SparkSession.builder.appName("Bespoke ETL Script") \
        .config("spark.driver.extraClassPath", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar") \
        .config("spark.jars", "/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar") \
        .config("spark.sql.debug.maxToStringFields", 2000000) \
        .getOrCreate()
    return spark1

def load_bespoke_data(spark, last_transaction_rec_id, batch_size=5000):
    processing_props = {
        "url": "jdbc:postgresql://196.46.20.76:1701/pattest",
        "user": "bespoke",
        "password": "Bes@123",
        "driver": "org.postgresql.Driver",
    }

    table_name1 = "switch.transactions"

    columns_to_select_from_ds = [
        'id', 'institution', 'beneficiarybank', 'country',
        'uan', 'currency', 'merchantid', 'terminalid',
        'channel', 'transactiontype', 'issuccessful', 'rrn'
    ]

    try:
        start_time = time.time()  # Start measuring time

        df = spark.read.jdbc(url=processing_props['url'], table=table_name1, properties=processing_props).select(columns_to_select_from_ds)

        df = df.orderBy('id')

        # Rename columns
        df = df.toDF(
            "department_table_id", "issuer_institution_id", "issuer_institution_name",
            "issuer_country_code", "pan", "account_currency",
            "merchant_id", "terminal_id", "channel", "transaction_type",
            "transaction_status", "rrn"
        )

        # Add new columns
        df = df.withColumn("department", lit("bespoke"))
        df = df.withColumn("status_code", lit(None).cast('string'))
        df = df.withColumn("id", monotonically_increasing_id() + (last_transaction_rec_id + 1))
        df = df.withColumn("settlement_status", lit(None).cast('string'))
        df = df.withColumn("transaction_date", lit(None).cast('string'))
        df = df.withColumn("merchant_name", lit(None).cast('string'))
        df = df.withColumn("terminal_owner", lit(None).cast('string'))
        df = df.withColumn("charged_back_or_disputed", lit(None).cast('string'))
        df = df.withColumn("domestic_or_foreign", lit(None).cast('string'))
        df = df.withColumn("row_num", lit(None).cast('string'))
        df = df.withColumn("acquirer_institution_id", lit(None).cast('string'))
        df = df.withColumn("acquirer_institution_name", lit(None).cast('string'))
        df = df.withColumn("acquirer_country_code", lit(None).cast('string'))
        df = df.withColumn("channel_secondary", lit(None).cast('string'))

        # Write to Data Warehouse
        row_count = df.count()
        print(f"")
        for i in range(0, row_count, batch_size):
            batch_df = df.limit(batch_size).filter(f"id > {last_transaction_rec_id + i}")
            batch_df.write.jdbc(url=pg_url, table='public.transaction', mode="append", properties=connection_properties)

        # Commit the transaction
        connection_properties['user'] = 'postgres'
        connection_properties['password'] = 'iamherenow'
        connection_properties['driver'] = 'org.postgresql.Driver'
        connection_properties['driverLocation'] = '/home/opc/etl_layer/db_drivers/postgresql-42.5.3.jar'
        conn = psycopg2.connect(**connection_properties)
        conn.commit()
        
        end_time = time.time()  # End measuring time
        elapsed_time = end_time - start_time
        print(f"load_bespoke_data took {elapsed_time:.2f} seconds")
        return True, df
    except Exception as err:
        print(f"Error during data transfer: {err}")
        return False, err


def main():
    try:
        start_time = time.time()  # Start measuring time
        conn = psycopg2.connect(
            dbname='etl_dwh_db',
            user='postgres',
            password='iamherenow',
            host='127.0.0.1',
            port='5432'
        )
        last_transaction_rec_id = get_last_transaction_id(conn)
        end_time = time.time()  # End measuring time
        elapsed_time = end_time - start_time
        print(f"get_last_transaction_id took {elapsed_time:.2f} seconds")

        start_time = time.time()  # Start measuring time
        spark = create_spark_session()
        success, df = load_bespoke_data(spark, last_transaction_rec_id)
        end_time = time.time()  # End measuring time
        elapsed_time = end_time - start_time
        print(f"load_bespoke_data took {elapsed_time:.2f} seconds")

        if success:
            print('Data transfer completed for bespoke')

            # Update the 'Tracker' table and process_record table with the last record's id
            start_time = time.time()  # Start measuring time
            cur = conn.cursor()
            cur.execute("SELECT id FROM transaction WHERE department = 'bespoke' ORDER BY id DESC LIMIT 1")
            transaction_last_record = cur.fetchone()
            
            print(transaction_last_record)
            if transaction_last_record:
                transaction_last_record_id = transaction_last_record[0]
                # Update process_record table
                cur.execute("UPDATE process_record SET record = %s WHERE script_name = 'bespoke'", (transaction_last_record_id,))
                conn.commit()

            cur.close()
            end_time = time.time()  # End measuring time
            elapsed_time = end_time - start_time
            print(f"Update and commit took {elapsed_time:.2f} seconds")

        conn.close()
        print("ETL process completed")
    except Exception as err:
        print(err)

if __name__ == "__main__":
    print("--------------- RUNNING ETL FOR BESPOKE ----------------")
    main()
