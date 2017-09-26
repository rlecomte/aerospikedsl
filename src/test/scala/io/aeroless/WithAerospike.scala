package io.aeroless

import com.spotify.docker.client.{DefaultDockerClient, DockerClient}
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.{DockerContainer, DockerFactory, DockerKit}

trait WithAerospike extends DockerKit {

  private val client: DockerClient = DefaultDockerClient.fromEnv().build()

  override implicit val dockerFactory: DockerFactory = new SpotifyDockerFactory(client)

  val aerospikeContainer = DockerContainer("aerospike:3.14.1.2")
    .withPorts(
      30000 -> None,
      30001 -> None,
      30002 -> None,
      30003 -> None
    )

  abstract override def dockerContainers: List[DockerContainer] = aerospikeContainer :: super.dockerContainers
}
