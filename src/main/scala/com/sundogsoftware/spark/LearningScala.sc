val hello: String = "Hola!!"

val pi: Float = 3.1415f
val doubled_pi: Double = pi * 2
println(f"doubled the pi $doubled_pi%.3f")


val number: Byte = 3
number match {
	case 1 => println("one")
	case 2 => println("two")
	case 3 => println("three")
	case _ => println("something else")
}

for(i <- 1 until 10; if i%2 == 0)
	yield i

for(i <- 1 to 10) println(i)
for(i <- 1 until 10) println(i)

{val x=10; 30 }


var num1:Int = 0; var num2:Int =1
var sum:Int =0
for (i <- 0 until 10) {
		println(num1)
		sum = num1 + num2
		num1 = num2
		num2 = sum
	}

def fib(num: Int=0):Int={
		if (num <= 1){
			return num
		}else {
			 return fib(num-1)+ fib(num - 2)
		}
}

println(fib(9))

def TransformInt(x:Int, f: Int => Int): Int = {
	f(x)
}

def squareInt(x: Int): Int = { x * x}

println(TransformInt(4, x => {x*x}))


println(TransformInt(4, x => {val y = x*2 ; y*y}))

val captainStuff = ("picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

"picard" -> "Enterprise-D"


val shiplist = List("Enterprise", "Defiant", "Voyager", "Deep space")

println(shiplist.head)
println(shiplist.tail)

shiplist.map((x:String) => x.reverse)








