// Companion objects are used to share methods and values

// that are not specific to instances of companion classes

case class Person(name: String) {
  def introduction: Unit = println(Person.commonGreeting + " I am " + name)
}

object Person {
  private val commonGreeting: String = "Hello!"
}

val person1 = Person("Jane Doe")
person1.introduction
// person1.commonGreeting
