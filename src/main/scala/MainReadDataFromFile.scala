import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object MainReadDataFromFile extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("MainReadDataFromFile")
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
  val customersDataframe = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(customerSchema)
    .csv(getClass.getResource("customers.csv").getPath)

  customersDataframe.show()

}
