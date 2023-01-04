package models

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

case class Purchase(id: String, customerId: String, products: Seq[String], price: Double)

object Purchase {
  implicit val encoder: Encoder[Purchase] = Encoders.product[Purchase]

  val schema: StructType = Encoders.product[Purchase].schema
}
