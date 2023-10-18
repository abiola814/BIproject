from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark MS-SQL Example") \
    .config("spark.jars", "mssql-jdbc-8.4.1.jre8.jar,spark-mssql-connector-1.0.2.jar") \
    .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://172.19.8.18:1700;databaseName=gateway_db") \
    .option("user", "sa") \
    .option("password", "vnp-1234") \
    .option("dbtable", "Transactions") \
    .load()
