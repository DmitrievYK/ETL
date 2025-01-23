/*
Задание 3
Определите в какой нормальной форме данная таблица, приведите её ко 2 и 3 нормальным формам последовательно.

C:\1\ETL\hw1
chcp 65001 && spark-shell -i C:\1\ETL\hw1\hw1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val t1 = System.currentTimeMillis()
if(1==1){
    var df1 = spark.read.option("delimiter"," ")
        .option("header", "true")
        .option("useHeader", "false")
        .csv("C:/1/ETL/hw1/profession.csv")

    df1.select("Employee_ID", "Job_Code", "City_code").distinct
    .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=1234&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_hw1_all")
    .mode("overwrite").save()

    df1.select("Employee_ID", "Name").distinct
    .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=1234&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_hw1_workerN")
    .mode("overwrite").save()

	df1.select("Job_Code", "Job").distinct
    .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=1234&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_hw1_prof")
    .mode("overwrite").save()

    df1.select("City_code", "Home_City").distinct
    .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=1234&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_hw1_city")
    .mode("overwrite").save()

	println("homework 1 complete")

    spark.stop()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)