package servicegraph

import domain.{Dependency, ServiceDefinition, TargetDependency}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import testutil.TestData

class ServiceDependencyGraphSpec extends AnyWordSpec with Matchers {

  private val graph =
    ServiceDependencyGraph(TestData.definitions, TestData.maxReplicaMap, TestData.minReplicaMap)

  "ServiceDependencyGraph.inferTargets" should {
    "return the dependencies of a service along with each dependency's own replica limits" in {
      graph.inferTargets("frontend") shouldBe Seq(
        TargetDependency("cartservice", maxReplicas = 5, minReplicas = 1),
        TargetDependency("checkoutservice", maxReplicas = 3, minReplicas = 2)
      )
    }

    "return no dependencies for a service that has none" in {
      graph.inferTargets("cartservice") shouldBe empty
    }

    "return no dependencies for a service it does not know about" in {
      graph.inferTargets("does-not-exist") shouldBe empty
    }

    "return no dependencies when built from no definitions" in {
      ServiceDependencyGraph(Seq.empty, Map.empty, Map.empty).inferTargets("frontend") shouldBe empty
    }
  }

  "ServiceDependencyGraph.mapServicesToDependencies" should {
    "contain an entry for every defined service" in {
      ServiceDependencyGraph
        .mapServicesToDependencies(TestData.definitions, TestData.maxReplicaMap, TestData.minReplicaMap)
        .keySet shouldBe Set("frontend", "cartservice", "checkoutservice")
    }

    "default replica limits to 1 for a dependency that has no definition" in {
      val definitions = Seq(
        ServiceDefinition("frontend", Seq(Dependency("undefinedservice", 1)), maxReplicas = 10, minReplicas = 1)
      )

      ServiceDependencyGraph
        .mapServicesToDependencies(definitions, Map("frontend" -> 10), Map("frontend" -> 1))("frontend") shouldBe
        Seq(TargetDependency("undefinedservice", maxReplicas = 1, minReplicas = 1))
    }
  }
}
