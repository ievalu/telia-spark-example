import Data.customerRows
import Data.purchaseRows
import models.Customer
import models.Purchase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when

object MainDataframe extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("MainDataframe")
    .getOrCreate()

  // Create spark DataFrames which then could be used in later transformations.
  val customerDataframe: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(customerRows), Customer.schema)

  val purchaseDataframe: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(purchaseRows), Purchase.schema)

  // Instead of one letter gender definition, give a proper category title.
  val customerDFWithGenderTitle = customerDataframe
    .withColumn(
      "genderTitle",
      when(col("gender").equalTo("f"), lit("female"))
        .when(col("gender").equalTo("m"), lit("male"))
        .otherwise("unknown")
    )

  // Show results with the new gender title
  customerDFWithGenderTitle.show()

  // Filter out male customers, leaving only female ones.
  val filteredCustomerDF = customerDFWithGenderTitle.filter(col("gender") === "f")

  // Join the two datasets to get all the purchases that female customers make.
  val joinedDataset: DataFrame =
    filteredCustomerDF
      .join(
        purchaseDataframe,
        filteredCustomerDF.col("id").equalTo(purchaseDataframe.col("customerId")),
        "inner"
      )
      .drop(filteredCustomerDF.col("id"))
      .drop(filteredCustomerDF.col("gender"))
      .drop(filteredCustomerDF.col("genderTitle"))
      .drop(filteredCustomerDF.col("name"))

  // Show the final result.
  joinedDataset.show()

}
