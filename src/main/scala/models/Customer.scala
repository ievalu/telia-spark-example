package models

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

case class Customer(id: String, name: String, gender: String)

object Customer {
  implicit val encoder: Encoder[Customer] = Encoders.product[Customer]

  val schema: StructType = Encoders.product[Customer].schema
}
