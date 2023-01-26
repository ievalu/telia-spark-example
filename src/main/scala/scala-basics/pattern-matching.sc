import scala.util.Random

val randomInt: Int = Random.nextInt(10)

// Match on random int
randomInt match {
  case 0 => "zero"
  case 1 => "one"
  case 2 => "two"
  case _ => "other"
}

sealed trait Reminder

case class Message() extends Reminder

case class PopUpNotification() extends Reminder

case class Email() extends Reminder

val reminder: Reminder = Message()

reminder match {
  case m: Message           => "message"
  case p: PopUpNotification => "pop up"
  case e: Email             => "email"
}
