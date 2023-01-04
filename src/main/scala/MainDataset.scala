import Data.customers
import Data.purchases
import models.Customer
import models.Purchase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object MainDataset extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("MainDataset")
    .getOrCreate()

  // Create sequences of fabricated objects.
  // In real world, data would come from an actual source.
  // See Data Object.

  // Create spark Datasets which then could be used in later transformations.
  val customerDataset: Dataset[Customer] = spark.createDataset(spark.sparkContext.parallelize(customers))
  val purchaseDataset: Dataset[Purchase] = spark.createDataset(spark.sparkContext.parallelize(purchases))

  // Instead of one letter gender definition, give a proper category title.
  def transformCustomer(customer: Customer): Customer = customer.gender match {
    case "f" => Customer(id = customer.id, name = customer.name, gender = "female")
    case "m" => Customer(id = customer.id, name = customer.name, gender = "male")
    case _   => Customer(id = customer.id, name = customer.name, gender = "unknown")
  }

  // Filter out male customers, leaving only female ones.
  val transformedCustomerDataset = customerDataset.filter(c => c.gender == "f").map(transformCustomer)

  // Join the two datasets to get all the purchases that female customers make.
  val joinedDataset: Dataset[Purchase] =
    customerDataset
      .join(purchaseDataset, customerDataset.col("id").equalTo(purchaseDataset.col("customerId")), "inner")
      .drop(customerDataset.col("id"))
      .drop(customerDataset.col("gender"))
      .drop(customerDataset.col("name"))
      .as[Purchase]

  // Show the final result.
  joinedDataset.show()

}
