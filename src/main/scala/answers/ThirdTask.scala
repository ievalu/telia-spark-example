package answers

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.StructType
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

// NOTE: You can solve this task using whichever data structure you prefer.

// Using files subscribers.csv; broadband-subscriptions.csv; tv-subscriptions.csv create transformed and aggregated
// dataset with type Dataset[Final].

// NOTE: product - value either BB or TV; stock, sales and churn - aggregated figures on product, segment and technology
// NOTE: for TV product technology is Unknown
object ThirdTask extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("ThirdTaskAnswer")
    .getOrCreate()

  case class Subscriber(id: String, firstName: String, lastName: String, gender: String, segment: String) {
    val name: String               = firstName + " " + lastName
    val genderTitle: String        = if (gender == "f") "female" else if (gender == "m") "male" else "unknown"
    val transformedSegment: String = if (segment == "B2C") "Consumer" else if (segment == "B2B") "Business" else "Other"
  }

  object Subscriber {
    implicit val encoder: Encoder[Subscriber] = Encoders.product[Subscriber]
    val schema: StructType                    = Encoders.product[Subscriber].schema
  }

  case class BroadbandSubscription(
      id: String,
      subscriberId: String,
      technology: String,
      validFrom: String,
      validTo: String) {
    val product: String = "BB"

    private val dateFormatter = new DateTimeFormatterBuilder()
      .appendPattern("yyyyMM")
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter()

    val validFromDate = LocalDate.parse(validFrom, dateFormatter)
    val validToDate   = LocalDate.parse(validTo, dateFormatter)

    private val currentDate = LocalDate.now().withDayOfMonth(1)

    val stock: Int =
      if (
        (validFromDate.isBefore(currentDate) || validFromDate.isEqual(currentDate)) && validToDate.isAfter(currentDate)
      ) 1
      else 0

    val sales: Int = if (validFrom == LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"))) 1 else 0
    val churn: Int = if (validTo == LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"))) 1 else 0
  }

  object BroadbandSubscription {
    implicit val encoder: Encoder[BroadbandSubscription] = Encoders.product[BroadbandSubscription]
    val schema: StructType                               = Encoders.product[BroadbandSubscription].schema
  }

  case class TVSubscription(
      id: String,
      subscriberId: String,
      validFrom: String,
      validTo: String) {
    val product: String    = "TV"
    val technology: String = "Unknown"

    private val dateFormatter = new DateTimeFormatterBuilder()
      .appendPattern("yyyyMM")
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter()

    val validFromDate = LocalDate.parse(validFrom, dateFormatter)
    val validToDate   = LocalDate.parse(validTo, dateFormatter)

    private val currentDate = LocalDate.now().withDayOfMonth(1)

    val stock: Int =
      if (
        (validFromDate.isBefore(currentDate) || validFromDate.isEqual(currentDate)) && validToDate.isAfter(currentDate)
      ) 1
      else 0

    val sales: Int = if (validFrom == LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"))) 1 else 0
    val churn: Int = if (validTo == LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"))) 1 else 0
  }

  object TVSubscription {
    implicit val encoder: Encoder[TVSubscription] = Encoders.product[TVSubscription]
    val schema: StructType                        = Encoders.product[TVSubscription].schema
  }

  case class TransformedSubscriber(id: String, name: String, gender: String, segment: String)

  object TransformedSubscriber {
    implicit val encoder: Encoder[TransformedSubscriber] = Encoders.product[TransformedSubscriber]
    val schema: StructType                               = Encoders.product[TransformedSubscriber].schema
  }

  case class TransformedBroadbandSubscription(
      id: String,
      subscriberId: String,
      product: String,
      technology: String,
      stock: Int,
      sales: Int,
      churn: Int)

  object TransformedBroadbandSubscription {
    implicit val encoder: Encoder[TransformedBroadbandSubscription] = Encoders.product[TransformedBroadbandSubscription]
    val schema: StructType = Encoders.product[TransformedBroadbandSubscription].schema
  }

  case class TransformedTVSubscription(
      id: String,
      subscriberId: String,
      product: String,
      technology: String,
      stock: Int,
      sales: Int,
      churn: Int)

  object TransformedTVSubscription {
    implicit val encoder: Encoder[TransformedTVSubscription] = Encoders.product[TransformedTVSubscription]
    val schema: StructType                                   = Encoders.product[TransformedTVSubscription].schema
  }

  val subscriberDataset: Dataset[Subscriber] =
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(Subscriber.schema)
      .csv("src/main/resources/subscribers.csv")
      .as[Subscriber]

  val broadbandSubscriptionDataset: Dataset[BroadbandSubscription] =
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(BroadbandSubscription.schema)
      .csv("src/main/resources/broadband-subscriptions.csv")
      .as[BroadbandSubscription]

  val tvSubscriptionDataset: Dataset[TVSubscription] =
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(TVSubscription.schema)
      .csv("src/main/resources/tv-subscriptions.csv")
      .as[TVSubscription]

  val transformedSubscriberDataset: Dataset[TransformedSubscriber] =
    subscriberDataset.map(s =>
      TransformedSubscriber(id = s.id, name = s.name, gender = s.genderTitle, segment = s.transformedSegment)
    )

  val transformedBroadbandSubscriptionDataset: Dataset[TransformedBroadbandSubscription] =
    broadbandSubscriptionDataset.map(b =>
      TransformedBroadbandSubscription(
        id = b.id,
        subscriberId = b.subscriberId,
        product = b.product,
        technology = b.technology,
        stock = b.stock,
        sales = b.sales,
        churn = b.churn
      )
    )

  val transformedTVSubscriptionDataset: Dataset[TransformedTVSubscription] = tvSubscriptionDataset.map(t =>
    TransformedTVSubscription(
      id = t.id,
      subscriberId = t.subscriberId,
      product = t.product,
      technology = t.technology,
      stock = t.stock,
      sales = t.sales,
      churn = t.churn
    )
  )

  val broadbandSubscriptions = transformedSubscriberDataset
    .join(
      transformedBroadbandSubscriptionDataset,
      transformedSubscriberDataset("id").equalTo(transformedBroadbandSubscriptionDataset("subscriberId"))
    )

  val tvSubscriptions = transformedSubscriberDataset
    .join(
      transformedTVSubscriptionDataset,
      transformedSubscriberDataset("id").equalTo(transformedTVSubscriptionDataset("subscriberId"))
    )

  val joined = broadbandSubscriptions.union(tvSubscriptions)

  val aggregated = joined
    .groupBy("product", "segment", "technology")
    .agg(sum("stock").alias("stock"), sum("sales").alias("sales"), sum("churn").alias("churn"))

  aggregated.show()
}
