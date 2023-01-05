package tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

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
    Row("6", "3", "Fiber", "202201", "444401"),
    Row("7", "8", "Fiber", "202201", "202401"),
    Row("8", "1", "Fiber", "202209", "202309"),
    Row("9", "7", "Fiber", "202106", "202201"),
    Row("10", "7", "Fiber", "202107", "202201"),
    Row("11", "2", "Fiber", "202201", "202301")
  )

  val firstDataframe: DataFrame  = ???
  val secondDataframe: DataFrame = ???
}
