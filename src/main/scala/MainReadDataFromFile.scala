import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object MainReadDataFromFile extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("MainReadDataFromFile")
    .config("spark.sql.parquet.compression.codec", "uncompressed")
    .getOrCreate()

  // Create schema
  val customerSchema: StructType = StructType(
    StructField("id", StringType, true) ::
      StructField("name", StringType, true) ::
      StructField("age", IntegerType, true) :: Nil
  )

  // Read data file with specific schema
  // Schema can be inferred
  // Different types of files can be read: csv, parquet, avro etc.
  val customersDataframe: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(customerSchema)
    .csv(getClass.getResource("customers.csv").getPath)

  customersDataframe.show()

  // Create a parquet file from data frame and then read it
  val transformedCustomersDF: DataFrame = customersDataframe
    .withColumn("temporary", functions.split(col("name"), " "))
    .select(
      col("id"),
      col("temporary").getItem(0).as("firstName"),
      col("temporary").getItem(1).as("lastName"),
      col("age")
    )

  transformedCustomersDF.write.parquet("src/main/resources/transformed-customers")

  // Read the previously written data and check schema and the data
  val readTransformedCustomersDF: DataFrame = spark.read
    .format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .parquet("src/main/resources/transformed-customers")

  readTransformedCustomersDF.printSchema
  readTransformedCustomersDF.show()

}
