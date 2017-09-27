package io.aeroless

import scala.concurrent.{ExecutionContext, Future, Promise}

import com.aerospike.client.listener.{RecordSequenceListener, WriteListener}
import com.aerospike.client.{AerospikeException, Key, Record}

import cats.data.Kleisli
import cats.~>
import io.aeroless.AerospikeIO.{Bind, FMap, Fail, GetAll, Join, Pure, Put, Query, ScanAll}

object KleisliInterpreter { module =>

  def apply(implicit ec: ExecutionContext): AerospikeIO ~> Kleisli[Future, AerospikeManager, ?] = λ[AerospikeIO ~> Kleisli[Future, AerospikeManager, ?]] {

    case Put(key, bins) => kleisli[Key] { m =>

      val promise = Promise[Key]()
      
      m.client.put(m.eventLoops.next(), new WriteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key): Unit = promise.success(key)

      }, null, key, bins: _*)
      
      
      promise.future
    }

    case Query(statement) => kleisli[Vector[(Key, Record)]] { m =>
      val promise = Promise[Vector[(Key, Record)]]()

        m.client.query(m.eventLoops.next(), new RecordSequenceListener {

          val results = Vector.newBuilder[(Key, Record)]

          override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

          override def onRecord(key: Key, record: Record): Unit = results += (key -> record)

          override def onSuccess(): Unit = promise.success(results.result())

        }, null, statement.toStatement)

      promise.future
    }

    case ScanAll(ns, set, bins) => kleisli[Vector[(Key, Record)]] { m =>
      val promise = Promise[Vector[(Key, Record)]]()

        m.client.scanAll(m.eventLoops.next(), new RecordSequenceListener {

          val results = Vector.newBuilder[(Key, Record)]

          override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

          override def onRecord(key: Key, record: Record): Unit = results += (key -> record)

          override def onSuccess(): Unit = promise.success(results.result())

        }, null, ns, set, bins: _*)

      promise.future
    }

    case GetAll(keys) => kleisli[Vector[(Key, Record)]] { m =>
      val promise = Promise[Vector[(Key, Record)]]()

        m.client.get(m.eventLoops.next(), new RecordSequenceListener {

          val results = Vector.newBuilder[(Key, Record)]

          override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

          override def onRecord(key: Key, record: Record): Unit = results += (key -> record)

          override def onSuccess(): Unit = promise.success(results.result())

        }, null, keys)

      promise.future
    }

    case Pure(x) => kleisli { _ => Future.successful(x) }

    case Join(opA, opB) => kleisli { m =>
      val f1: Future[Any] = module.apply(ec)(opA)(m)
      val f2: Future[Any] = module.apply(ec)(opB)(m)
      for {
        a <- f1
        b <- f2
      } yield (a, b)
    }

    case Bind(x, f) => kleisli { m =>
      module.apply(ec)(x)(m).flatMap(r => module.apply(ec)(f(r))(m))

    }

    case FMap(x, f) => kleisli { m =>
      module.apply(ec)(x)(m).map(f)
    }

    case Fail(t) => kleisli { _ => Future.failed(t) }
  }


  private def kleisli[A](f: AerospikeManager => Future[A]): Kleisli[Future, AerospikeManager, A] = Kleisli.apply[Future, AerospikeManager, A](f)
}