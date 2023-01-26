import models.Customer
import util.TestBase

class MainDatasetSpec extends TestBase {

  val customer: Customer = Customer(id = "1", name = "Name Last", gender = "m")

  "MainDatasetSpec" should {
    "correctly transform customer" in {
      val transformedCustomer = MainDataset.transformCustomer(customer)

      transformedCustomer.gender shouldBe "male"
    }
  }

}
