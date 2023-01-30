package answers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

// Steps:
//   1. Create dataframe of subscribers with properties: id, firstName, lastName, gender, segment
//   2. Create dataframe of broadband subscriptions with properties: id, subscriberId, technology, validFrom, validTo
//   3. Transform subscriber dataframe:
//     a. Concatenate firstName and lastName to make one property - name
//     b. Transform the gender property to have values of female, male, unknown
//     c. Transform segment property to be: B2C - Consumer, B2B - Business or Other
//   4. Transform broadband subscription dataframe:
//     a. Add new property - stock, which is 1 when the current date is between validFrom and validTo, otherwise 0
//     b. Add new property - sales, which is 1 when the validFrom and current date is the same month of the same year, otherwise 0
//     c. Add new property - churn, which is 1 when the validTo and current date is the same month of the same year, otherwise 0
//   5. Join the two dataframes using appropriate property
object FirstTask extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("FirstTaskAnswer")
    .getOrCreate()

  val subscribers: Seq[Row] = Seq(
    Row("1", "Jane", "Doe", "f", "B2C"),
    Row("2", "John", "White", "m", "B2C"),
    Row("3", "Chris", "Wright", "m", "B2B"),
    Row("4", "Mary", "Gold", "f", "B2B"),
    Row("5", "Harry", "Potter", "m", "B2C"),
    Row("6", "Noel", "Olsen", "m", "B2C"),
    Row("7", "Bella", "Evans", "f", "B2O"),
    Row("8", "Will", "Brown", "m", "B2B")
  )

  val broadbandSubscriptions: Seq[Row] = Seq(
    Row("1", "1", "Fiber", "202209", "202210"),
    Row("2", "5", "Fiber", "202107", "202207"),
    Row("3", "7", "Fiber", "202106", "202206"),
    Row("4", "2", "Fiber", "202209", "444409"),
    Row("5", "4", "Fiber", "202201", "202301"),
    Row("6", "3", "Fiber", "202301", "444401"),
    Row("7", "8", "Fiber", "202301", "202401"),
    Row("8", "1", "Fiber", "202209", "202309"),
    Row("9", "7", "Fiber", "202106", "202201"),
    Row("10", "7", "Fiber", "202107", "202201"),
    Row("11", "2", "Fiber", "202201", "202301")
  )

  val subscriberSchema: StructType = StructType(
    StructField("id", StringType, true) ::
      StructField("firstName", StringType, true) ::
      StructField("lastName", StringType, true) ::
      StructField("gender", StringType, true) ::
      StructField("segment", StringType, true) :: Nil
  )

  val broadbandSubscriptionSchema: StructType = StructType(
    StructField("id", StringType, true) ::
      StructField("subscriberId", StringType, true) ::
      StructField("technology", StringType, true) ::
      StructField("validFrom", StringType, true) ::
      StructField("validTo", StringType, true) :: Nil
  )

  val subscriberDataframe: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(subscribers), subscriberSchema)

  val broadbandSubscriptionDataframe: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(broadbandSubscriptions), broadbandSubscriptionSchema)

  val transformedSubscriberDf = subscriberDataframe
    .withColumn("name", functions.concat(col("firstName"), lit(" "), col("lastName")))
    .withColumn(
      "gender",
      when(col("gender").equalTo("f"), lit("female"))
        .when(col("gender").equalTo("m"), lit("male"))
        .otherwise("unknown")
    )
    .withColumn(
      "segment",
      when(col("segment").equalTo("B2C"), lit("Consumer"))
        .when(col("segment").equalTo("B2B"), lit("Business"))
        .otherwise("Other")
    )
    .drop("firstName")
    .drop("lastName")

  val transformedBroadbandSubscriptionDf = broadbandSubscriptionDataframe
    .withColumn(
      "stock",
      when(
        to_date(col("validFrom"), "yyyyMM") <= current_date() and to_date(col("validTo"), "yyyyMM") >= current_date(),
        1
      ).otherwise(0)
    )
    .withColumn(
      "sales",
      when(
        year(to_date(col("validFrom"), "yyyyMM")) === year(current_date()) and month(
          to_date(col("validFrom"), "yyyyMM")
        ) === month(current_date()),
        1
      ).otherwise(0)
    )
    .withColumn(
      "churn",
      when(
        year(to_date(col("validTo"), "yyyyMM")) === year(current_date()) and month(
          to_date(col("validTo"), "yyyyMM")
        ) === month(current_date()),
        1
      ).otherwise(0)
    )
    .withColumnRenamed("id", "broadBandSubscriptionId")

  val joinedDataframe = subscriberDataframe
    .join(
      transformedBroadbandSubscriptionDf,
      subscriberDataframe("id").equalTo(transformedBroadbandSubscriptionDf("subscriberId"))
    )
    .drop("id")

  joinedDataframe.show()

}
