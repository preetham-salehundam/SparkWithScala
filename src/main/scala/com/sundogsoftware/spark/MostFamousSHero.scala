package com.sundogsoftware.spark

import scala.io.{Source, Codec}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, col, min}

object MostFamousSHero {

//	final case class SuperHeroes
	
	def loadHeroNames():Map[Int, String] = {
		implicit val codec:Codec = Codec("iso-8859-1")
		val lines = Source.fromFile("data/Marvel-names.txt")
		var superHeroes: Map[Int, String] = Map()
		for (line <- lines.getLines) {
			val records = line.split("\"")
			if (records.length > 1)
				superHeroes += (records(0).trim.toInt -> records(1))
			else
				superHeroes += (records(0).trim.toInt -> "")
		}
		lines.close()
		superHeroes
	}
		
		def main(args: Array[String]): Unit = {
			
			Logger.getLogger("org").setLevel(Level.ERROR)
			
			val spark = SparkSession.builder
				.appName("SuperHeroes")
				.master("local[*]")
				.getOrCreate()
			import spark.implicits._
			
			val superheroLookup = spark.sparkContext.broadcast(loadHeroNames())
			
			val superheroNetwork = spark.sparkContext.textFile("data/Marvel-graph.txt")
			
			val popularSuperHeroIds = superheroNetwork.map(x => x.split("\\s+"))
				.map(x => (x(0).toInt, x.length - 1))
				.reduceByKey((x,y) => x+y)
				.toDF( "heroID", "cooccurance_count")
			
			def name_lookup():Int => String = (heroId: Int) => superheroLookup.value(heroId)
			
			def name_lookup_udf = udf(name_lookup)
			
			val popularSuperHeroes = popularSuperHeroIds.withColumn("SuperHeroName", name_lookup_udf(col("heroId")))
			
			val min_connections = popularSuperHeroes.orderBy($"cooccurance_count").select("cooccurance_count").limit(1).collect()
			print(min_connections)
			val obscureSuperHeroes = popularSuperHeroes.filter($"cooccurance_count" === min_connections(0)(0))

			obscureSuperHeroes.show(obscureSuperHeroes.count.toInt)
			spark.stop()
		}
	}