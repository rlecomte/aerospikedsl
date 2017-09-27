package io.aeroless


import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy, EventLoops, NettyEventLoops}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

import io.netty.channel.nio.NioEventLoopGroup

class Fs2Spec extends FlatSpec with Matchers with BeforeAndAfterAll with GivenWhenThen with ScalaFutures {

  case class TestValue(id: String)
  "Value" should "be read" in {
    import connection._

    implicit val aerospikeClient: AsyncClient = new AsyncClient(new AsyncClientPolicy(), "localhost", 3000)

    val kd = keydomain("test", "set")

    val io = for {
      _ <- put(kd("test1"), TestValue("value1"))
      _ <- put(kd(1L), TestValue("value2"))
      _ <- put(kd(1), TestValue("value3"))
      _ <- put(kd("test3"), TestValue("value4"))
      record <- query[TestValue](statement("test", "set").readBins("id"))
    } yield record

    import scala.concurrent.ExecutionContext.Implicits.global
    val result = io.runFuture(new AerospikeManager {
      override val eventLoops: EventLoops = new NettyEventLoops(new NioEventLoopGroup(1))

      override val client: AsyncClient = aerospikeClient
    })

    result.onComplete { r => info(r.toString) }
  }

}
