// map function
val ints       = Seq(1, 2, 3)
val doubleInts = (x: Int) => x * 2
val newInts    = ints.map(doubleInts)

// Pass anonymous function to map
val ints2    = Seq(1, 2, 3)
val newInts2 = ints2.map(x => x * 2)

// Pass anonymous function to map
val ints3    = Seq(1, 2, 3)
val newInts3 = ints2.map(_ * 2)

// Custom functions that accept functions
def sale(prices: Seq[Double], saleFunction: Double => Double): Seq[Double] =
  prices.map(saleFunction)

def tenPercent(number: Double): Double = number * 0.9

def fiftyPercent(number: Double): Double = number * 0.5

val prices                    = Seq(100.0, 130.0, 200.0)
val tenPercentReducedPrices   = sale(prices, tenPercent)
val fiftyPercentReducedPrices = sale(prices, fiftyPercent)

// Functions that return functions
def urlBuilder(ssl: Boolean, domainName: String): (String, String) => String = {
  val schema = if (ssl) "https://" else "http://"
  (endpoint: String, query: String) => s"$schema$domainName/$endpoint?$query"
}

val domainName = "www.example.com"
def getURL     = urlBuilder(ssl = true, domainName)
val endpoint   = "users"
val query      = "id=1"
val url        = getURL(endpoint, query)
