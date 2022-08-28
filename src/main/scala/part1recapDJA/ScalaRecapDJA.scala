package part1recapDJA

import scala.concurrent.Future

object ScalaRecapDJA extends App {

  // declaring values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello, Scala") // Unit = "No meaningful value = void in other languages"

  // functions
  def myFunction(x: Int) = 42

  // OOP

  class Animal
  class Dog extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch")
  }


  // singleton pattern
  object MySingleton


  // companions
  object Carnivore

  // generics

  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming

  // Lambda
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)
  println(incremented)

  val processedList = List(1,2,3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife)
  }


}
