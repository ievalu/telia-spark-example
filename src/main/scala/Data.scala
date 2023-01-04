import models.Customer
import models.Purchase
import org.apache.spark.sql.Row

object Data {

  val customerRows: Seq[Row] = Seq(
    Row("1", "Jane Dow", "f"),
    Row("2", "Joe Malone", "m")
  )

  val purchaseRows: Seq[Row] = Seq(
    Row("1", "1", Seq.empty, 0.0),
    Row("2", "1", Seq.empty, 0.0),
    Row("3", "1", Seq.empty, 0.0),
    Row("4", "2", Seq.empty, 0.0),
    Row("5", "2", Seq.empty, 0.0)
  )

  val customers: Seq[Customer] =
    Seq(Customer(id = "1", name = "Jane Doe", gender = "f"), Customer(id = "2", name = "Joe Malone", gender = "m"))

  val purchases: Seq[Purchase] = Seq(
    Purchase(id = "1", customerId = "1", products = Seq.empty, price = 0.0),
    Purchase(id = "2", customerId = "1", products = Seq.empty, price = 0.0),
    Purchase(id = "3", customerId = "1", products = Seq.empty, price = 0.0),
    Purchase(id = "4", customerId = "2", products = Seq.empty, price = 0.0),
    Purchase(id = "5", customerId = "2", products = Seq.empty, price = 0.0)
  )

}
