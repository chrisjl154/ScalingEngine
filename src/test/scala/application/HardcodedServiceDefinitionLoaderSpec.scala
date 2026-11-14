package application

import domain.{Dependency, ServiceDefinition}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HardcodedServiceDefinitionLoaderSpec extends AnyWordSpec with Matchers {

  "HardcodedServiceDefinitionLoader.loadAll" should {
    "load every service from the bundled definitions file" in {
      val definitions = HardcodedServiceDefinitionLoader.loadAll("definitions/services.json").unsafeRunSync()

      definitions.map(_.serviceName) should contain theSameElementsAs Seq(
        "frontend",
        "checkoutservice",
        "cartservice",
        "adservice",
        "productcatalogservice",
        "recommendationservice",
        "shippingservice",
        "currencyservice",
        "paymentservice",
        "emailservice"
      )
    }

    "decode dependencies and replica limits" in {
      val definitions = HardcodedServiceDefinitionLoader.loadAll("definitions/services.json").unsafeRunSync()
      val frontend    = definitions.find(_.serviceName == "frontend").get

      frontend.maxReplicas shouldBe 10
      frontend.minReplicas shouldBe 1
      frontend.dependencies should contain(Dependency("cartservice", 1))
    }

    "skip lines that are not valid service definitions and keep the rest" in {
      HardcodedServiceDefinitionLoader.loadAll("definitions/mixed-validity.json").unsafeRunSync() shouldBe Seq(
        ServiceDefinition("frontend", Seq(Dependency("cartservice", 1)), maxReplicas = 10, minReplicas = 1),
        ServiceDefinition("cartservice", Seq.empty, maxReplicas = 5, minReplicas = 1)
      )
    }

    "fail when the definitions file does not exist" in {
      HardcodedServiceDefinitionLoader.loadAll("definitions/does-not-exist.json").attempt.unsafeRunSync() shouldBe a[
        Left[_, _]
      ]
    }
  }
}
