package com.sundogsoftware.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, IntegerType, TimestampType}
import org.apache.log4j._
import org.apache.spark.sql.functions.{desc, udf}
import scala.io.{Codec, Source}
object popular_movies {
	
	final case class Movies(userId: Int, movieId : Int, ratings:Int, timestamp: Long)
	
	
	def loadmovienames(): Map[Int, String] = {
		/**
		 * This method locally loads the entire lookup data
		 * @returns Map of Id and Movie Names
		 * @author Preetham Salehundam
		 * */
		
		implicit val codec:Codec = Codec("ISO-8859-1")
		
		val lines = Source.fromFile("data/ml-100k/u.item")
		
		var movie_name_lookup:Map[Int, String] = Map()
		for (line <- lines.getLines()){
			val columns = line.split('|')
			if (columns.length > 1) {
				movie_name_lookup += (columns(0).toInt -> columns(1))
			}
		}
		lines.close()
		
		movie_name_lookup
	}
	
	
	
	def main(args: Array[String]): Unit = {
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		val spark = SparkSession.builder.appName("movies").master("local[*]").getOrCreate()
		try {
			val movieSchema = new StructType()
				.add("userID", IntegerType, nullable = false)
				.add("movieID", IntegerType, nullable = false)
				.add("ratings", IntegerType, nullable = true)
				.add("timestamp", TimestampType, nullable = true)
			
			import spark.implicits._
			val movie_ds = spark.read
				.option("sep", "\t")
				.schema(movieSchema)
				.csv("data/ml-100k/u.data")
				.as[Movies]
			
			val nameDict = spark.sparkContext.broadcast(loadmovienames())
			
			def lookup_names(): Int => String = (movieID: Int) => nameDict.value(movieID)
			
			val name_lookup = udf(lookup_names)
			val results = movie_ds
				.coalesce(10).groupBy("movieID").count().withColumn("movieName", name_lookup($"movieID")).orderBy(desc("count"))
			
			results.show(false)
		}
		finally{
			spark.stop()
		}
		
	}
	
}
