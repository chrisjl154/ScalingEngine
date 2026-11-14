package stream

import application.HardcodedServiceDefinitionLoader
import cats.effect.{ContextShift, IO, Timer}
import config.StreamConfig
import domain._
import fs2.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import servicegraph.ServiceDependencyGraph
import skuber.apps.v1.Deployment
import testutil.{FakeKubernetesAPI, TestData}

import scala.concurrent.ExecutionContext

class PrometheusAnomalyStreamSpec extends AnyWordSpec with Matchers {
  implicit private val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit private val timer: Timer[IO]     = IO.timer(ExecutionContext.global)

  private val streamConfig = StreamConfig(
    bootstrapServer = "localhost:9092",
    consumerGroup = "test-group",
    topic = TestData.topic,
    streamSleepTime = 5,
    streamParallelismMax = 4
  )

  private val graph =
    ServiceDependencyGraph(TestData.definitions, TestData.maxReplicaMap, TestData.minReplicaMap)

  /** Builds a stream over the test service graph, backed by a fake cluster holding `deployments` */
  private def streamWith(deployments: Deployment*): (PrometheusAnomalyStream, FakeKubernetesAPI) = {
    val api = new FakeKubernetesAPI(deployments.map(d => d.name -> d).toMap)
    (PrometheusAnomalyStream(graph, streamConfig, api, TestData.maxReplicaMap, TestData.minReplicaMap), api)
  }

  private val (stream, _) = streamWith()

  private def candidate(name: String, function: String): ScalingCandidate =
    ScalingCandidate(name, TestData.maxReplicaMap(name), TestData.minReplicaMap(name), function)

  "processKafkaRecord" should {
    "decode a valid anomaly message and keep the record key" in {
      stream.processKafkaRecord(TestData.record("key-1", TestData.anomalyJson("frontend", "out"))).unsafeRunSync() shouldBe
        Some(
          KeyedAnomalyMessage(
            "key-1",
            AnomalyMessage("frontend", "0.97", "out", AnomalyMessageMetaData("1605312000000"))
          )
        )
    }

    "return None for a value that is not JSON" in {
      stream.processKafkaRecord(TestData.record("key-1", "not json")).unsafeRunSync() shouldBe None
    }

    "return None for JSON that is missing required fields" in {
      stream.processKafkaRecord(TestData.record("key-1", """{"targetAppName":"frontend"}""")).unsafeRunSync() shouldBe None
    }

    "return None for an empty value" in {
      stream.processKafkaRecord(TestData.record("key-1", "")).unsafeRunSync() shouldBe None
    }
  }

  "getMinAndMaxReplicas" should {
    "return the configured limits for a known service" in {
      stream.getMinAndMaxReplicas("checkoutservice") shouldBe ((2, 3))
    }

    "default both limits to 1 for an unknown service" in {
      stream.getMinAndMaxReplicas("does-not-exist") shouldBe ((1, 1))
    }
  }

  "createAnomaly" should {
    "attach the target's replica limits and its dependencies" in {
      val message = KeyedAnomalyMessage("k", AnomalyMessage("frontend", "0.97", "out", AnomalyMessageMetaData("0")))

      stream.createAnomaly(message).unsafeRunSync() shouldBe Anomaly(
        "frontend",
        Seq(
          TargetDependency("cartservice", maxReplicas = 5, minReplicas = 1),
          TargetDependency("checkoutservice", maxReplicas = 3, minReplicas = 2)
        ),
        maxReplicas = 10,
        minReplicas = 1,
        function = "out"
      )
    }

    "use default limits and no dependencies for an unknown service" in {
      val message = KeyedAnomalyMessage("k", AnomalyMessage("mystery", "0.97", "in", AnomalyMessageMetaData("0")))

      stream.createAnomaly(message).unsafeRunSync() shouldBe
        Anomaly("mystery", Seq.empty, maxReplicas = 1, minReplicas = 1, function = "in")
    }
  }

  "createScalingCandidates" should {
    "return the target first, followed by each dependency, all with the anomaly's direction" in {
      val anomaly = Anomaly(
        "frontend",
        Seq(TargetDependency("cartservice", 5, 1), TargetDependency("checkoutservice", 3, 2)),
        maxReplicas = 10,
        minReplicas = 1,
        function = "in"
      )

      stream.createScalingCandidates(anomaly).unsafeRunSync() shouldBe Seq(
        ScalingCandidate("frontend", 10, 1, "in"),
        ScalingCandidate("cartservice", 5, 1, "in"),
        ScalingCandidate("checkoutservice", 3, 2, "in")
      )
    }

    "return only the target when it has no dependencies" in {
      stream.createScalingCandidates(Anomaly("cartservice", Seq.empty, 5, 1, "out")).unsafeRunSync() shouldBe
        Seq(ScalingCandidate("cartservice", 5, 1, "out"))
    }
  }

  "getCurrentDeployment" should {
    "pair the candidate with its deployment when it exists" in {
      val cart        = TestData.deployment("cartservice", 2)
      val (s, _)      = streamWith(cart)
      val cartTarget  = candidate("cartservice", "out")

      s.getCurrentDeployment(cartTarget).unsafeRunSync() shouldBe Some(TargetAndDeployment(cartTarget, cart))
    }

    "return None when the deployment does not exist" in {
      stream.getCurrentDeployment(candidate("cartservice", "out")).unsafeRunSync() shouldBe None
    }
  }

  "getReplicasFromDeployment" should {
    "return the deployment's replica count" in {
      stream.getReplicasFromDeployment(TestData.deployment("cartservice", 4)).unsafeRunSync() shouldBe 4
    }

    "default to 1 when the deployment does not state a replica count" in {
      stream
        .getReplicasFromDeployment(TestData.deploymentWithoutReplicaCount("cartservice"))
        .unsafeRunSync() shouldBe 1
    }
  }

  "scaleServiceAndDependencies" when {
    "scaling out" should {
      "scale up a service that is below its maximum" in {
        val cart   = TestData.deployment("cartservice", 2)
        val (s, k) = streamWith(cart)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("cartservice", "out"), cart)).unsafeRunSync()

        k.scaleUps shouldBe List("cartservice" -> 2)
        k.scaleDowns shouldBe empty
      }

      "scale up a service that is one below its maximum" in {
        val cart   = TestData.deployment("cartservice", 4)
        val (s, k) = streamWith(cart)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("cartservice", "out"), cart)).unsafeRunSync()

        k.scaleUps shouldBe List("cartservice" -> 4)
      }

      "not scale a service that is at its maximum" in {
        val cart   = TestData.deployment("cartservice", 5)
        val (s, k) = streamWith(cart)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("cartservice", "out"), cart)).unsafeRunSync()

        k.scaleUps shouldBe empty
        k.scaleDowns shouldBe empty
      }

      "not scale a service that is already above its maximum" in {
        val cart   = TestData.deployment("cartservice", 7)
        val (s, k) = streamWith(cart)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("cartservice", "out"), cart)).unsafeRunSync()

        k.scaleUps shouldBe empty
        k.scaleDowns shouldBe empty
      }
    }

    "scaling in" should {
      "scale down a service that is above its minimum" in {
        val checkout = TestData.deployment("checkoutservice", 3)
        val (s, k)   = streamWith(checkout)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("checkoutservice", "in"), checkout))
          .unsafeRunSync()

        k.scaleDowns shouldBe List("checkoutservice" -> 3)
        k.scaleUps shouldBe empty
      }

      "not scale a service that is at its minimum" in {
        val checkout = TestData.deployment("checkoutservice", 2)
        val (s, k)   = streamWith(checkout)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("checkoutservice", "in"), checkout))
          .unsafeRunSync()

        k.scaleUps shouldBe empty
        k.scaleDowns shouldBe empty
      }

      "not scale a service that is already below its minimum" in {
        val checkout = TestData.deployment("checkoutservice", 1)
        val (s, k)   = streamWith(checkout)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("checkoutservice", "in"), checkout))
          .unsafeRunSync()

        k.scaleUps shouldBe empty
        k.scaleDowns shouldBe empty
      }
    }

    // The two tests below describe the intended behaviour, which the code does not have yet.
    // They report as pending while the bug exists, and fail once it is fixed so the marker can be removed.
    "given an unrecognised direction" should {
      "not scale the service" in pendingUntilFixed {
        val cart   = TestData.deployment("cartservice", 3)
        val (s, k) = streamWith(cart)

        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("cartservice", "sideways"), cart))
          .unsafeRunSync()

        // Currently anything other than "out" is treated as "in", so this scales down
        k.scaleDowns shouldBe empty
        k.scaleUps shouldBe empty
      }
    }

    "given a deployment without a replica count" should {
      "not throw and not scale" in pendingUntilFixed {
        val cart   = TestData.deploymentWithoutReplicaCount("cartservice")
        val (s, k) = streamWith(cart)

        // Currently throws NoSuchElementException from `replicas.get`
        s.scaleServiceAndDependencies(TargetAndDeployment(candidate("cartservice", "out"), cart)).unsafeRunSync()

        k.scaleUps shouldBe empty
      }
    }
  }

  "processRecords (the full pipeline without Kafka)" should {
    def run(s: PrometheusAnomalyStream, values: String*): Unit =
      s.processRecords(
          Stream.emits(values.zipWithIndex.map { case (v, i) => TestData.record(s"key-$i", v, i.toLong) })
        )
        .compile
        .drain
        .unsafeRunSync()

    "scale out the anomalous service and all of its dependencies" in {
      val (s, k) = streamWith(
        TestData.deployment("frontend", 3),
        TestData.deployment("cartservice", 1),
        TestData.deployment("checkoutservice", 2)
      )

      run(s, TestData.anomalyJson("frontend", "out"))

      k.scaleUps should contain theSameElementsAs List(
        "frontend"        -> 3,
        "cartservice"     -> 1,
        "checkoutservice" -> 2
      )
      k.scaleDowns shouldBe empty
    }

    "scale in the anomalous service and all of its dependencies" in {
      val (s, k) = streamWith(
        TestData.deployment("frontend", 3),
        TestData.deployment("cartservice", 2),
        TestData.deployment("checkoutservice", 3)
      )

      run(s, TestData.anomalyJson("frontend", "in"))

      k.scaleDowns should contain theSameElementsAs List(
        "frontend"        -> 3,
        "cartservice"     -> 2,
        "checkoutservice" -> 3
      )
      k.scaleUps shouldBe empty
    }

    "scale only the services that are within their limits" in {
      val (s, k) = streamWith(
        TestData.deployment("frontend", 3),
        TestData.deployment("cartservice", 5),    // at max
        TestData.deployment("checkoutservice", 3) // at max
      )

      run(s, TestData.anomalyJson("frontend", "out"))

      k.scaleUps shouldBe List("frontend" -> 3)
    }

    "skip dependencies whose deployment cannot be found and still scale the rest" in {
      val (s, k) = streamWith(TestData.deployment("frontend", 3), TestData.deployment("checkoutservice", 2))

      run(s, TestData.anomalyJson("frontend", "out"))

      k.lookups should contain("cartservice")
      k.scaleUps should contain theSameElementsAs List("frontend" -> 3, "checkoutservice" -> 2)
    }

    "drop malformed records and keep processing the valid ones" in {
      val (s, k) = streamWith(TestData.deployment("cartservice", 1))

      run(s, "garbage", TestData.anomalyJson("cartservice", "out"), """{"targetAppName":"cartservice"}""")

      k.scaleUps shouldBe List("cartservice" -> 1)
    }

    "do nothing for an anomaly on a service that has no deployment" in {
      val (s, k) = streamWith()

      run(s, TestData.anomalyJson("mystery", "out"))

      k.lookups shouldBe List("mystery")
      k.scaleUps shouldBe empty
      k.scaleDowns shouldBe empty
    }

    "do nothing when there are no records" in {
      val (s, k) = streamWith(TestData.deployment("frontend", 3))

      run(s)

      k.lookups shouldBe empty
    }

    // Describes intended behaviour: with the bundled definitions, frontend lists checkoutservice twice,
    // so one anomaly currently scales checkoutservice twice.
    "scale each dependency at most once per anomaly when using the bundled definitions" in pendingUntilFixed {
      val definitions = HardcodedServiceDefinitionLoader.loadAll("definitions/services.json").unsafeRunSync()
      val maxMap      = definitions.map(d => d.serviceName -> d.maxReplicas).toMap
      val minMap      = definitions.map(d => d.serviceName -> d.minReplicas).toMap
      val api = new FakeKubernetesAPI(definitions.map(d => d.serviceName -> TestData.deployment(d.serviceName, 1)).toMap)
      val s = PrometheusAnomalyStream(ServiceDependencyGraph(definitions, maxMap, minMap), streamConfig, api, maxMap, minMap)

      run(s, TestData.anomalyJson("frontend", "out"))

      val names = api.scaleUps.map(_._1)
      names.distinct.size shouldBe names.size
    }
  }
}
