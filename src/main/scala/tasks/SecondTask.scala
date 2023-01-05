package tasks

import org.apache.spark.sql.Dataset

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
  case class Subscriber()
  case class BroadbandSubscription()

  val subscriberDataset: Dataset[Subscriber]                       = ???
  val broadbandSubscriptionDataset: Dataset[BroadbandSubscription] = ???
}
