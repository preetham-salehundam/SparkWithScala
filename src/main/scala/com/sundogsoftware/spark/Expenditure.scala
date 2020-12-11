package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, DoubleType, StructType}
import org.apache.spark.sql.functions.{round, sum ,desc}


object Expenditure {
	
	case class Expenditure(cust_id:Int, item_id: Int, amount_spent:Double)
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)
		val spark = SparkSession.builder.appName("Expenses")
			.master("local[*]")
			.getOrCreate()
		
		val expensesSchema = new StructType()
			.add("cust_id", IntegerType, nullable = false)
			.add("item_id", IntegerType, nullable=false)
			.add("amount_spent", DoubleType, nullable = true)
		
		import spark.implicits._
		val expenditures = spark.read
			.schema(expensesSchema)
			.csv("data/customer-orders.csv")
		
		val totalamount = expenditures.select("cust_id", "amount_spent")
			.groupBy("cust_id").agg(round(sum($"amount_spent"),2).alias("total_expenditure")).sort(desc("total_expenditure"))
		
		totalamount.show()
		
		
	}
	
	
}
