// Implicit parameters
case class Prefix(prefix: String)
def addPrefix(s: String)(implicit p: Prefix) = p.prefix + s

// then probably in your application
implicit val implicitPrefix: Prefix = Prefix("#")
addPrefix("value")
addPrefix("value")(Prefix("*"))

// Implicit conversions
implicit def doubleToInt(d: Double): Int = d.toInt
val int: Int                             = 42.0
val int2: Int                            = 42.6
