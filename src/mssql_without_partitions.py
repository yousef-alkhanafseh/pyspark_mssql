import os
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession

jars_dir = os.path.join(os.getcwd().replace("src", ""), "jars")
jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
jdbc_driver_path = ",".join(jdbc_driver_path)

start_time = time.time()

conf = SparkConf().setMaster("local[*]").set("spark.sql.debug.maxToStringFields", 1000) \
                                    .set("spark.executor.heartbeatInterval", 200000) \
                                    .set("spark.network.timeout", 300000) \
                                    .set("spark.sql.execution.arrow.pyspark.enabled", "true") \
                                    .set("spark.jars", jdbc_driver_path) \
                                    .set("spark.ui.port",4040) \
                                    .set("spark.driver.cores", "5") \
                                    .set("spark.executor.cores", "5") \
                                    .set("spark.driver.memory", "1G")  \
                                    .set("spark.executor.memory", "1G")  \
                                    .set("spark.executor.instances", "2") \
                                    .setAppName("PYSPARK_MSSQL_TUTORIAL")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

db_ip = "<db_ip>"
db_name = "<db_name>"
db_username = "<db_username>"
db_password = "<db_password>"
table_name = "<table_name>"

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbc_url = "jdbc:sqlserver://{};databaseName={};user={};password={};".format(db_ip, db_name, db_username, db_password)

main_query = f"(SELECT *, ROW_NUMBER() OVER (ORDER BY username) AS row_num FROM {table_name}) as my_table"
df = spark.read \
    .format("jdbc") \
    .option("url",jdbc_url) \
    .option("driver", driver) \
    .option("dbtable", main_query) \
    .load()

final_df = df.groupBy("username").count()
final_df = final_df.orderBy("count", ascending=False)

final_df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", jdbc_url.replace(db_name, "TnTemp")) \
        .option("dbtable", table_name + "_USER_GROUPED_TEMP") \
        .option("isolationLevel", "NONE") \
        .option("rewriteBatchedStatements", "true") \
        .mode("overwrite") \
        .save()

end_time = time.time()
print("Execution time with partitioning: {} s". format(round(end_time - start_time, 4)))