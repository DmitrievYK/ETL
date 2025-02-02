import pyspark, time, platform, sys, os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import inspect, create_engine
from pandas.io import sql
import warnings, matplotlib
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
import MySQLdb


warnings.filterwarnings("ignore")
t0 = time.time()
t0 = time.time()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

con = create_engine("mysql://root:1234@localhost/spark")


spark = SparkSession.builder \
    .appName("sem4") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

w = Window.partitionBy(lit(1)).orderBy("num").rowsBetween(Window.unboundedPreceding, Window.currentRow)

def create_table(table_name):
        sql.execute(f"""drop table if exists {table_name}""", con)
        sql.execute(f"""CREATE TABLE if not exists {table_name} (\
                num INT(10) NULL DEFAULT NULL,\
                mon DATE NULL DEFAULT NULL,\
                sum FLOAT NULL DEFAULT NULL,\
                debt_sum FLOAT NULL DEFAULT NULL,\
                persent_sum FLOAT NULL DEFAULT NULL,\
                debt_left FLOAT NULL DEFAULT NULL,\
                percent_summary FLOAT NULL DEFAULT NULL,\
                debt_summary FLOAT NULL DEFAULT NULL
                )\
                ENGINE=InnoDB""", con)

def load_data_to_table(file_name, table_name):
        df = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load(file_name).limit(1000)\
        .withColumn("percent_summary", sum1(col("percent_sum")).over(w))\
        .withColumn("debt_summary", sum1(col("debt_sum")).over(w))

        df.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=1234&useUnicode=true&serverTimezone=UTC\
                                &sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", table_name)\
        .mode("overwrite").save()
        return df

def add_plot(df, color1, color2):
        print(df.head())
        df = df.toPandas()
        # Get current axis 
        ax = plt.gca()
        ax.ticklabel_format(style='plain')
        # bar plot
        df.plot(kind='line', 
                x='num', 
                y='debt_summary', 
                color=color1, ax=ax)
        df.plot(kind='line', 
                x='num', 
                y='percent_summary', 
                color=color2, ax=ax)
    


if 1==2:
        create_table("spark.etl_4_2a")
        create_table("spark.etl_4_2b")
        create_table("spark.etl_4_2c")

df1 = load_data_to_table("sem4_2_1.xlsx","etl_4_2a")
df2 = load_data_to_table("sem4_2_2.xlsx","etl_4_2b")
df3 = load_data_to_table("sem4_2_3.xlsx","etl_4_2c")

print("DF1:")
df1.show(2)
print("DF2:")
df2.show(2)
print("DF3:")
df3.show(2)


add_plot(df1, "green", "red")
add_plot(df2, "blue", "cyan")
add_plot(df3, "magenta", "yellow")

# show the plot
plt.legend(['долг_86689', 'проценты_86689','долг_120000', 'проценты_120000','долг_150000', 'проценты_150000'])
plt.show()

spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))