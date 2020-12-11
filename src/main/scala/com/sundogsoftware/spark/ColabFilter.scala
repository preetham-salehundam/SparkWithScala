package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, LongType}

object ColabFilter {
	/**
	 * load data into schemas
	 * find movies paris rated by same user - by self join - cache()
	 * find cosine similarities for moviePairs dataset
	 * find the movie pairs most similar with (> 0.97)
	 * find co-occurance count, num of time each pair appeared and threshold it to >50
	 *
	 * */
	
	case class Movies(userID: Int, movieID: Int, rating: Int)
	case class MoviesNames(movieID: Int, movieTitle: String)
	case class MoviePairs(Movie1: Int, Movie2: Int, UserID:Int)
	case class MovieSimilarityScores(Movie1:Int, Movie2: Int, score:Double, numOfPairs:Int)

	def main(args: Array[String]): Unit ={

		Logger.getLogger("org").setLevel(Level.ERROR)

		


		val MovieSchema = StructType(
			Seq(
				 StructField("userID", IntegerType, nullable=true),
				 StructField("movieID", IntegerType, nullable=true),
				 StructField("rating", IntegerType, nullable=true)
			)
		)

		val MovieNamesSchema =  StructType(
			Seq(
				 StructField("movieID", IntegerType, nullable=false),
				 StructField("movieTitle", StringType, nullable=true)
			)
		)

		val sc = SparkSession.builder.appName("MovieColab").master("local[*]").getOrCreate()

		import sc.implicits._
		val movies = sc.read
			.option("delimiter", "\\t")
			//.option("charset", "ISO-8859-1")
			.schema(MovieSchema)
			.csv("data/ml-100k/u.data")
			.as[Movies]

		val movieNames = sc.read
			.option("sep", "|")
			.option("charset", "ISO-8859-1")
			.schema(MovieNamesSchema)
			.text("data/ml-100k/u.item")
	//		.as[MoviesNames]

		val ratings = movies.select("userID", "movieID", "rating")


		val movieNamePairs = movies.as("ratings_1")
			.join(movies.as("ratings_2"), $"ratings_1.userID" === $"ratings_2.userID" && $"ratings_1.movieID" < $"ratings_2.movieID")
			.select($"ratings_1.movieID".alias("movie_1"),
				$"ratings_2.movieID".alias("movie_2"),
				$"ratings_1.rating".alias("rating_1"),
				$"ratings_2.rating".alias("rating_2"))
			//.as[MoviePairs]


//		def computeCosine(sc:SparkSession, data:Dataset[MoviePairs]): Dataset[MovieSimilarityScores] = {
//			val pairsScore = data
//				.withColumn("xy", col("rating_1") * col("rating_2") )
//				.withColumn("xx", col("rating_1") * col("rating_1") )
//				.withColumn("yy", col("rating_2") * col("rating_2"))
//
//			val cosineScores = data.select()
//		}


}

}
