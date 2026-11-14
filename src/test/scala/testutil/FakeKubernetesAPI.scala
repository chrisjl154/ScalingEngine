package testutil

import cats.effect.IO
import kube.KubernetesAPI
import skuber.apps.v1.Deployment

import scala.collection.mutable.ListBuffer

/**
 * In-memory KubernetesAPI that records every scaling call instead of talking to a cluster.
 * Calls may arrive concurrently from the stream, so all access to the buffers is synchronised.
 *
 * @param deployments deployments that "exist" in the fake cluster, keyed by name
 */
class FakeKubernetesAPI(deployments: Map[String, Deployment]) extends KubernetesAPI {
  private val lookupsBuffer  = ListBuffer.empty[String]
  private val scaleUpsBuffer = ListBuffer.empty[(String, Int)]
  private val scaleDownsBuffer = ListBuffer.empty[(String, Int)]

  override def getDeploymentByName(name: String, namespace: String): IO[Option[Deployment]] =
    IO {
      synchronized(lookupsBuffer += name)
      deployments.get(name)
    }

  override def scaleUp(deployment: Deployment, currentReplicas: Int): IO[Unit] =
    IO(synchronized(scaleUpsBuffer += (deployment.name -> currentReplicas)))

  override def scaleDown(deployment: Deployment, currentReplicas: Int): IO[Unit] =
    IO(synchronized(scaleDownsBuffer += (deployment.name -> currentReplicas)))

  /** Names of every deployment that was looked up */
  def lookups: List[String] = synchronized(lookupsBuffer.toList)

  /** (deployment name, replicas at the time of the call) for every scale-up */
  def scaleUps: List[(String, Int)] = synchronized(scaleUpsBuffer.toList)

  /** (deployment name, replicas at the time of the call) for every scale-down */
  def scaleDowns: List[(String, Int)] = synchronized(scaleDownsBuffer.toList)
}
