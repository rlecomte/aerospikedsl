package io.aeroless


import scala.concurrent.ExecutionContext

import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy, EventLoops, NettyEventLoops}
import com.aerospike.client.{Bin, Key}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

import io.netty.channel.nio.NioEventLoopGroup

class Fs2Spec extends FlatSpec with Matchers with BeforeAndAfterAll with GivenWhenThen with ScalaFutures with WithAerospike {

  //implicit val pc = PatienceConfig(Span(120, Seconds), Span(1, Second))

  case class TestValue(id: String)
  "Value" should "be read" in {
    import connection._
    //dockerContainers.forall(_.isReady().futureValue) shouldBe true

    implicit val aerospikeClient: AsyncClient = new AsyncClient(new AsyncClientPolicy(), "localhost", 3000)

    //put(key(0), new Bin("id", "value")).map(v => println(s"PUT $v")).run.unsafeRunSync()

    //getAll(key).map(v => println(s"RESULT!!!! $v")).run.unsafeRunSync()

    val key1 = new Key("test", "set", "test1")
    val key2 = new Key("test", "set", "test2")
    val key3 = new Key("test", "set", "test3")
    val key4 = new Key("test", "set", "test4")

    val io = for {
      _ <- put(key1, TestValue("value1"))
      //_ <- put(key2, new Bin("id", "value2"))
      //_ <- put(key3, new Bin("id", "value3"))
      //_ <- put(key4, new Bin("id", "value4"))
      record <- query(QueryStatement("test", "set").readBins("id")).decodeOrFail[TestValue]
      //record <- scanAll("test", "set", "id" :: Nil).decodeOrFail[TestValue]
    } yield record

    import scala.concurrent.ExecutionContext.Implicits.global
    val result = KleisliInterpreter.apply(io).apply(new AerospikeManager {
      override val eventLoops: EventLoops = new NettyEventLoops(new NioEventLoopGroup(1))

      override val client: AsyncClient = aerospikeClient

      override val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
    })

    result.onComplete { r => info(r.toString) }

    "Hello" should be equals("Hello")
  }
}
