package answers

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import java.time.LocalDate
import java.time.format.DateTimeFormatter

// Steps:
//   1. Create dataset of subscribers from subscribers.csv file
//   2. Create dataset of broadband subscriptions from broadband-subscriptions.csv file
//   3. Transform subscriber dataset:
//     a. Concatenate firstName and lastName to make one property - name
//     b. Transform the gender property to have values of female, male, unknown
//     c. Transform segment property to be: B2C - Consumer, B2B - Business or Other
//   4. Transform broadband subscription dataset:
//     a. Add new property - stock, which is 1 when the current date is between validFrom and validTo, otherwise 0
//     b. Add new property - sales, which is 1 when the validFrom and current date is the same month of the same year, otherwise 0
//     c. Add new property - churn, which is 1 when the validTo and current date is the same month of the same year, otherwise 0
//   5. Join the two datasets using appropriate property
object SecondTask extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("SecondTaskAnswer")
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
    val stock: Int = 1
    val sales: Int = if (validFrom == LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"))) 1 else 0
    val churn: Int = if (validTo == LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"))) 1 else 0
  }

  object BroadbandSubscription {
    implicit val encoder: Encoder[BroadbandSubscription] = Encoders.product[BroadbandSubscription]
    val schema: StructType                               = Encoders.product[BroadbandSubscription].schema
  }

  case class TransformedSubscriber(id: String, name: String, gender: String, segment: String)

  object TransformedSubscriber {
    implicit val encoder: Encoder[TransformedSubscriber] = Encoders.product[TransformedSubscriber]
    val schema: StructType                               = Encoders.product[TransformedSubscriber].schema
  }

  case class TransformedBroadbandSubscription(
      id: String,
      subscriberId: String,
      technology: String,
      stock: Int,
      sales: Int,
      churn: Int)

  object TransformedBroadbandSubscription {
    implicit val encoder: Encoder[TransformedBroadbandSubscription] = Encoders.product[TransformedBroadbandSubscription]
    val schema: StructType = Encoders.product[TransformedBroadbandSubscription].schema
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

  val transformedSubscriberDataset: Dataset[TransformedSubscriber] =
    subscriberDataset.map(s =>
      TransformedSubscriber(id = s.id, name = s.name, gender = s.genderTitle, segment = s.transformedSegment)
    )

  val transformedBroadbandSubscriptionDataset: Dataset[TransformedBroadbandSubscription] =
    broadbandSubscriptionDataset.map(b =>
      TransformedBroadbandSubscription(
        id = b.id,
        subscriberId = b.subscriberId,
        technology = b.technology,
        stock = b.stock,
        sales = b.sales,
        churn = b.churn
      )
    )

  val joinedDataset = transformedSubscriberDataset.join(
    transformedBroadbandSubscriptionDataset,
    transformedSubscriberDataset("id").equalTo(transformedBroadbandSubscriptionDataset("subscriberId"))
  )

  joinedDataset.show()

}
