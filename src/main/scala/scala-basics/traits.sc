trait Employee {
  // abstract members
  val id: Int
  val firstName: String
  val lastName: String
  // concrete members
  def name: String = firstName + " " + lastName
}

case class SoftwareEngineer(id: Int, firstName: String, lastName: String) extends Employee

val softwareEngineer = SoftwareEngineer(1, "Jane", "Doe")
softwareEngineer.name
