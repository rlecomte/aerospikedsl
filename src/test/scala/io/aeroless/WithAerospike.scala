package io.aeroless

import com.aerospike.client.async._
import com.spotify.docker.client.{DefaultDockerClient, DockerClient}

import io.netty.channel.nio.NioEventLoopGroup

trait WithAerospike /*extends DockerKit */{

  private val client: DockerClient = DefaultDockerClient.fromEnv().build()

  //override implicit val dockerFactory: DockerFactory = new SpotifyDockerFactory(client)

  //implicit val aerospikeClient: AerospikeClient = new AerospikeClient("localhost", 3000)

  implicit val eventLoop: EventLoops = new NettyEventLoops(new NioEventLoopGroup(1))

  /*val aerospikeContainer = DockerContainer("aerospike:3.14.1.2")
    .withPorts(
      30000 -> None,
      30001 -> None,
      30002 -> None,
      30003 -> None
    )

  abstract override def dockerContainers: List[DockerContainer] = aerospikeContainer :: super.dockerContainers*/
}
