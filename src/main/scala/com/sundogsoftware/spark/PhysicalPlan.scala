package com.sundogsoftware.spark


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PhysicalPlan {
	
	case class Book(value:String)
	def main(args: Array[String]){
		
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		val sc = SparkSession.builder
			.appName("Plans")
			.master("local[*]")
			.config("spark.eventLog.enabled", true)
			.config("spark.eventLog.dir", "/tmp/spark-events")
			//.config("spark.eventLog.overwrite", true)
			.getOrCreate()
		
//		val data = sc.sparkContext.textFile("data/book.txt")
//		val words = data.flatMap(x => x.split("\\W+"))
		import sc.implicits._
		val data = sc.read.text("data/book.txt").as[Book]
		
		val words = data.select(explode(split($"value", "\\W+")).alias("word"))
			.select(lower($"word")
			.alias("word"))
			.filter($"word" =!= "")
			.groupBy("word")
			.count()
			.sort(desc("count"))
		
		
		
		val records = words.coalesce(1)
		print(records.rdd.getNumPartitions)
		records.explain()
		
		
		
		
		
		
		sc.stop()
		
	}
	
	
}
