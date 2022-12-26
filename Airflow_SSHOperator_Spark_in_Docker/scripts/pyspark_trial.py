from pyspark.sql import SparkSession
import pandas as pd
import sys
import time

print("Script started")

file_path=r'/opt/bitnami/spark/dev/scripts'
database = 'docker_trial'

host=sys.argv[1]
port=sys.argv[2]
login=sys.argv[3]
password=sys.argv[4]

spark = SparkSession\
        .builder\
        .appName("Airflow_Pyspark")\
        .config("spark.eventLog.enabled", "true")\
        .config("spark.driver.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:PrintGCTimeStamps")\
        .config("spark.executor.extraJavaOptions","-verbose:gc -XX:+PrintGCDetails -XX:PrintGCTimeStamps")\
        .getOrCreate()

sc=spark.sparkContext

print(spark)

#read data from csv 
data = pd.read_csv(file_path + r'/insert_data.csv', header=0)

df = spark.createDataFrame(data)

#adding some sleep time to see the spark applications web ui
time.sleep(30)

#write data into mysql
df.write.format("jdbc")\
    .mode('append')\
    .option('url', f"jdbc:mysql://{host}:{port}/{database}")\
    .option('user', login)\
    .option('dbtable', 'spark_table')\
    .option('password', password)\
    .option('driver','com.mysql.cj.jdbc.Driver')\
    .save()

print("Script completed")
