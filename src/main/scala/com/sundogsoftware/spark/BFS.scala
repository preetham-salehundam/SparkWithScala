package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer

object BFS {
	
	final val WHITE = "WHITE"
	final val GRAY = "GRAY"
	final val BLACK = "BLACK"

	
	def main(args: Array[String]):Unit = {
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		
		val startCharacterID = 5306 //SpiderMan
		val targetCharacterID = 14 //ADAM 3,031 (who?)
		
		var hitCounter:Option[LongAccumulator] = None
		
		type BFSData = (Array[Int], Int, String )
		
		type BFSNode = (Int, BFSData)
		
		
		def bfsParse(line:String):BFSNode = {
			//val lines = sc.textFile("data/Marvel-graph.txt")
			val fields = line.split("\\s+")
			val heroId = fields(0).toInt
			val connections = fields.drop(1).map(x => x.toInt)
			var color = GRAY
			//val distance = 1
			//if you start with this node, make the color gray
			if (heroId == startCharacterID)
				color = GRAY
			else
				color = WHITE
			
			(heroId, (connections, 9999, color))
		}
		
		def buildNetwork(sc: SparkContext): RDD[BFSNode]={
			val graph = sc.textFile("data/Marvel-graph.txt")
			graph.map(bfsParse)
		}
		
		def bfsMap(Hero: BFSNode):Array[BFSNode] = {
//			val sc = new SparkContext("local[*]", "BFS")
//			val superHeroNx = buildNetwork(sc)
			val heroid = Hero._1
			val heroData = Hero._2
			val connections = heroData._1
			var distance = heroData._2
			var color = heroData._3
			
			var results:ArrayBuffer[BFSNode]= ArrayBuffer()
			if (color == GRAY){
				//explore its connections and create new nodes
				//while exploring if you see target node raise a flag
				for (connection <- connections){
					if (connection ==  targetCharacterID) {
						if (hitCounter.isDefined){
							hitCounter.get.add(1)
						}
						val newHeroId = connection
						val newConnections = Array()
						val newDistance = distance + 1
						val newColor = GRAY
						
						results += new BFSNode(newHeroId, new BFSData(Array(), newDistance, newColor))
					}
					
					// set the color of explored node to BLACK
					color = BLACK
					
					// make a copy of the current node, set the color as black
					results += new BFSNode(heroid, new BFSData(Array(), 0, BLACK))
				}
				
			}
			
			if (heroid == startCharacterID) {
				// distance to itself is 0
				distance = 0
				val newNode = new BFSNode(heroid, new BFSData(Array(), distance, BLACK))
				results += newNode
			}
			
			results.toArray
		}
		
		
//		def bfsReduce(node_a: BFSNode, node_b: BFSNode): BFSNode = {
//			val heroid_a = node_a._1
//			val connections_a = node_a._2._1
//			val distance_a = node_a._2._2
//			val color_a = node_a._2._3
//			// ============================== //
//			val heroid_b = node_b._1
//			val connections_b = node_b._2._1
//			val distance_b = node_b._2._2
//			val color_b = node_b._2._3
//
//
//
//
////			if color_a
////			var shortestDistance = 0
////			if (distance_a < distance_b){
////				shortestDistance = distance_a
////			}
//
//
//		}
		
		
		val sc = new SparkContext("local[*]", "BFS")
		
		val network = buildNetwork(sc)
		
		println(network.collect().foreach(x => println(x._1, x._2, x._2._1, x._2._2, x._2._3)))
		
		//network.flatMap(bfsMap).foreach(x => println(x._1, x._2, x._2._1, x._2._2, x._2._3))
		
		
	}
	
}
