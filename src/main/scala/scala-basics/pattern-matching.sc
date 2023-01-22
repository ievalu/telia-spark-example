import scala.util.Random

val randomInt: Int = Random.nextInt(10)

// Match on random int
randomInt match {
  case 0 => "zero"
  case 1 => "one"
  case 2 => "two"
  case _ => "other"
}
