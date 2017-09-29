package io.aeroless

import scala.collection.immutable.IntMap.Bin

import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy, EventLoops, NettyEventLoops}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

import io.netty.channel.nio.NioEventLoopGroup

class OpsSpec extends FlatSpec with Matchers with BeforeAndAfterAll with GivenWhenThen with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global
  import connection._
  import org.scalatest.time._

  implicit val patience = PatienceConfig.apply(timeout = Span(10, Seconds), interval = Span(300, Millis))

  val aerospikeClient: AsyncClient = new AsyncClient(new AsyncClientPolicy(), "localhost", 3000)

  val manager = new AerospikeManager {
    override val eventLoops: EventLoops = new NettyEventLoops(new NioEventLoopGroup(1))

    override val client: AsyncClient = aerospikeClient
  }

  val kd = keydomain("test", "set")
  case class TestValue(id: String)
  case class LongValue(value: Long)

  "Append / Prepend" should "work" in {
    val key = kd("AppendOps")
    val io = for {
      _ <- put(key, TestValue("value"))
      _ <- append(key, Map(
        "id" -> "_with_suffix"
      ))
      _ <- prepend(key, Map(
        "id" -> "with_prefix_"
      ))
      v <- get[TestValue](key, Seq("id"))
    } yield v

    whenReady(io.runFuture(manager)) { r =>
      r should equal(Some(TestValue("with_prefix_value_with_suffix")))
    }
  }

  "Add operation" should "work" in {
    val key = kd("AddOps")
    val io = for {
      _ <- put(key, LongValue(1L))
      _ <- add(key, Seq(("value" -> 2L)))
      v <- get[LongValue](key, Seq("value"))
    } yield v

    whenReady(io.runFuture(manager)) { r =>
      r should equal(Some(LongValue(3L)))
    }
  }

  "Delete operation" should "work" in {
    val key = kd("AddOps")
    val io = for {
      _ <- put(key, TestValue("value"))
      _ <- delete(key)
      v <- get[TestValue](key, Seq("value"))
    } yield v

    whenReady(io.runFuture(manager)) { r =>
      r should equal(None)
    }
  }

  "Query statement operation" should "work" in {
    val io = for {
      _ <- put(kd("test_stmt_1"), TestValue("stmt1"))
      _ <- put(kd("test_stmt_2"), TestValue("stmt2"))
      _ <- put(kd("test_stmt_3"), TestValue("stmt3"))
      _ <- put(kd("test_stmt_4"), TestValue("stmt4"))
      record <- query[TestValue](statement("test", "set").readBins("id"))
    } yield record.map(_._2)


    whenReady(io.runFuture(manager)) { records =>
      records should contain allElementsOf Seq(
        TestValue("stmt1"),
        TestValue("stmt2"),
        TestValue("stmt4"),
        TestValue("stmt4")
      )
    }
  }

  "scan all operation" should "work" in {
    val io = for {
      _ <- put(kd("test_scan_1"), TestValue("scan1"))
      _ <- put(kd("test_scan_2"), TestValue("scan2"))
      _ <- put(kd("test_scan_3"), TestValue("scan3"))
      record <- scanAll[TestValue]("test", "set", "id" :: Nil)
    } yield record.map(_._2)


    whenReady(io.runFuture(manager)) { records =>
      records should contain allElementsOf Seq(
        TestValue("scan1"),
        TestValue("scan2"),
        TestValue("scan3")
      )
    }
  }

  "Get all operation" should "work" in {
    val key1 = kd("test_getall_1")
    val key2 = kd("test_getall_2")

    val io = for {
      _ <- put(key1, TestValue("getall1"))
      _ <- put(key2, TestValue("getall2"))
      record <- getAll[TestValue](Seq(key1, key2))
    } yield record.map(_._2)

    whenReady(io.runFuture(manager)) { records =>
      records should contain theSameElementsAs Seq(
        TestValue("getall1"),
        TestValue("getall2")
      )
    }
  }
}
