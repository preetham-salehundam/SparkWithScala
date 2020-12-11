package com.sundogsoftware.spark

object ScalaTutorial {
  def main(args: Array[String]): Unit ={
      var randsent = "I saw a dragon fly by"
      println("3rd index " + randsent(3))
      println(randsent.concat(" yesterday"))
      println("dragon starts at index " + randsent.indexOf("dragon"))
      val randsentArray = randsent.toArray
      for (i <- randsentArray) {
        println(i)
      }
    //println(Array(1,2,3,4))
    
//    def getSum(num1: Int = 1, num2: Int = 2): Int = {
//      return num1 + num2
//    }
    def getSum(args: Int*): Int = {
      var sum: Int = 0
      for (i <- args){
        sum += i
      }
      return sum
    }
  
    println(getSum(5,4,5,3,4,2))
  
    def factorial(num: BigInt): BigInt ={
      if (num <= 1)
        return 1
      else
        return num * factorial(num - 1)
    }
    
    println("factorial " +  factorial(1))
    
    // Array Buffer = variable length
    // Array - fixed lenght
    
  }
  
  
  
  
}
