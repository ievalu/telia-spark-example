package tasks

// NOTE: You can solve this task using whichever data structure you prefer.

// Using files subscribers.csv; broadband-subscriptions.csv; tv-subscriptions.csv create transformed and aggregated
// dataset with type Dataset[Final].

// NOTE: period - current YYYYMM; product - value either BB or TV; stock, sales and churn - aggregated figures on period, product, segment and technology
// NOTE: for TV product technology is Unknown
object ThirdTask extends App {

  case class Final(
      period: String,
      product: String,
      segment: String,
      technology: String,
      stock: Int,
      sales: Int,
      churn: Int)

}
