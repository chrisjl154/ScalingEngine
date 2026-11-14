package testutil

import cats.effect.IO
import domain.{Dependency, ServiceDefinition}
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, ConsumerRecord}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import skuber.apps.v1.Deployment

object TestData {
  val topic = "anomalies"

  /** A small version of the real service graph: frontend depends on cart and checkout */
  val definitions: Seq[ServiceDefinition] = Seq(
    ServiceDefinition(
      "frontend",
      Seq(Dependency("cartservice", 1), Dependency("checkoutservice", 1)),
      maxReplicas = 10,
      minReplicas = 1
    ),
    ServiceDefinition("cartservice", Seq.empty, maxReplicas = 5, minReplicas = 1),
    ServiceDefinition("checkoutservice", Seq.empty, maxReplicas = 3, minReplicas = 2)
  )

  val maxReplicaMap: Map[String, Int] = definitions.map(d => d.serviceName -> d.maxReplicas).toMap
  val minReplicaMap: Map[String, Int] = definitions.map(d => d.serviceName -> d.minReplicas).toMap

  def deployment(name: String, replicas: Int): Deployment =
    Deployment(name).withReplicas(replicas)

  /** A deployment whose spec exists but does not state a replica count */
  def deploymentWithoutReplicaCount(name: String): Deployment = {
    val d = Deployment(name).withReplicas(1)
    d.copy(spec = d.spec.map(_.copy(replicas = None)))
  }

  def anomalyJson(target: String, function: String): String =
    s"""{"targetAppName":"$target","metricResult":"0.97","function":"$function","meta":{"timeDetectedMillis":"1605312000000"}}"""

  def record(key: String, value: String, offset: Long = 0L): CommittableConsumerRecord[IO, String, String] =
    CommittableConsumerRecord(
      ConsumerRecord(topic, 0, offset, key, value),
      CommittableOffset[IO](
        new TopicPartition(topic, 0),
        new OffsetAndMetadata(offset),
        Some("test-group"),
        _ => IO.unit
      )
    )
}
