package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType, FloatType}
import org.apache.spark.sql.functions.{desc, round}
import scala.math.{max, min}

object Min_Max_Dataset {
	case class temperatures(station_id:String, date: Int, measure_type:String, temperature:Float)
	def main(args: Array[String]):Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)
		val spark = SparkSession.builder
			.appName("min_max")
			.master("local[*]")
			.getOrCreate()
		
		val temperaturesSchema = StructType(
			Seq(
			StructField("station_id", StringType, nullable = true),
			StructField("date", IntegerType, nullable = true),
			StructField("measure_type", StringType, nullable = true),
			StructField("temperature", FloatType, nullable = true),
		)
		)
		
		import spark.implicits._
		val temperatures = spark.read
			.schema(temperaturesSchema)
			.csv("data/1800.csv")
			.as[temperatures]
		
		val minTempRecords = temperatures.filter($"measure_type" === "TMIN")
		
		val minTempColumns = minTempRecords.select("station_id", "temperature")
		
		val results = minTempColumns.groupBy("station_id").min("temperature")
			.withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
			.select("station_id", "temperature").sort(desc("temperature"))
		
		results.show()
		
	}
	
	
}
