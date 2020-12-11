package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, col, regexp_extract, window, current_timestamp}
//import org.apache.spark._
object StreamingDemo {

def main(args: Array[String]): Unit = {
	
	Logger.getLogger("org").setLevel(Level.ERROR)
	
	val sc = SparkSession.builder
		.appName("Spark Streaming")
		.master("local[*]")
		.getOrCreate()

	
	val access_logs = sc.readStream.text("data/logs")
	
	// Regular expressions to extract pieces of Apache access log lines
	val contentSizeExp = "\\s(\\d+)$"
	val statusExp = "\\s(\\d{3})\\s"
	val generalExp = "\"(\\S+)\\s(\\S+)\\s*(\\S*)\""
	val timeExp = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]"
	val hostExp = "(^\\S+\\.[\\S+\\.]+\\S+)\\s"
	
	
	// Apply these regular expressions to create structure from the unstructured text
	val logsDF = access_logs.select(regexp_extract(col("value"), hostExp, 1).alias("host"),
		regexp_extract(col("value"), timeExp, 1).alias("timestamp"),
		regexp_extract(col("value"), generalExp, 1).alias("method"),
		regexp_extract(col("value"), generalExp, 2).alias("endpoint"),
		regexp_extract(col("value"), generalExp, 3).alias("protocol"),
		regexp_extract(col("value"), statusExp, 1).cast("Integer").alias("status"),
		regexp_extract(col("value"), contentSizeExp, 1).cast("Integer").alias("content_size"))
	
	
	
	//val statusCountsDF = logsDF.groupBy("status").count()
	
	//
	val modifiedLogsDF = logsDF.withColumn("event-time", current_timestamp())
	val popularEndPoint = modifiedLogsDF.groupBy(window(col("event-time"), "30 seconds", "10 seconds"), col("endpoint")).count().orderBy(desc("count"))
	
	
	val query = popularEndPoint.writeStream.outputMode("complete").format("console").queryName("counts").start()
	
	
	query.awaitTermination()
	
	sc.stop()
	
	
	
	
	
	
	
}
	
}
