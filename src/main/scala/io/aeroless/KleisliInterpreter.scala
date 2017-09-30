package io.aeroless

import scala.concurrent.{ExecutionContext, Future, Promise}

import com.aerospike.client.listener.{WriteListener, _}
import com.aerospike.client.{AerospikeException, Key, Record}

import cats.data.Kleisli
import cats.~>
import io.aeroless.AerospikeIO.{Add, Append, Bind, Delete, Exists, FMap, Fail, Get, GetAll, Header, Join, Prepend, Pure, Put, Query, ScanAll, Touch}

object KleisliInterpreter { module =>

  def apply(implicit ec: ExecutionContext): AerospikeIO ~> Kleisli[Future, AerospikeManager, ?] = λ[AerospikeIO ~> Kleisli[Future, AerospikeManager, ?]] {

    case Put(key, bins) => kleisli[Key] { m =>

      val promise = Promise[Key]()
      
      m.client.put(m.eventLoops.next(), new WriteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key): Unit = promise.success(key)

      }, m.writePolicy.orNull, key, bins: _*)
      
      
      promise.future
    }

    case Append(key, bins) => kleisli[Key] { m =>
      val promise = Promise[Key]

      m.client.append(m.eventLoops.next(), new WriteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key): Unit = promise.success(key)

      }, m.writePolicy.orNull, key, bins: _*)

      promise.future
    }

    case Prepend(key, bins) => kleisli[Key] { m =>
      val promise = Promise[Key]

      m.client.prepend(m.eventLoops.next(), new WriteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key): Unit = promise.success(key)

      }, m.writePolicy.orNull, key, bins: _*)

      promise.future
    }

    case Add(key, bins) => kleisli[Key] { m =>
      val promise = Promise[Key]

      m.client.add(m.eventLoops.next(), new WriteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key): Unit = promise.success(key)

      }, m.writePolicy.orNull, key, bins: _*)

      promise.future
    }

    case Delete(key) => kleisli[Key] { m =>
      val promise = Promise[Key]

      m.client.delete(m.eventLoops.next(), new DeleteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key, existed: Boolean): Unit = promise.success(key)

      }, m.writePolicy.orNull, key)

      promise.future
    }

    case Touch(key) => kleisli[Key] { m =>
      val promise = Promise[Key]

      m.client.touch(m.eventLoops.next(), new WriteListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key): Unit = promise.success(key)

      }, m.writePolicy.orNull, key)

      promise.future
    }

    case Exists(key) => kleisli[Boolean] { m =>
      val promise = Promise[Boolean]

      m.client.exists(m.eventLoops.next(), new ExistsListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key, exists: Boolean): Unit = promise.success(exists)
      }, m.batchPolicy.orNull, key)

      promise.future
    }

    case Get(key, bins) => kleisli[Option[Record]] { m =>
      val promise = Promise[Option[Record]]

      m.client.get(m.eventLoops.next(), new RecordListener {
        override def onFailure(exception: AerospikeException) = promise.failure(exception)

        override def onSuccess(key: Key, record: Record) = promise.success(Option(record))
      }, m.policy.orNull, key, bins: _*)

      promise.future
    }

    case Query(statement) => kleisli[Vector[(Key, Record)]] { m =>
      val promise = Promise[Vector[(Key, Record)]]()

        m.client.query(m.eventLoops.next(), new RecordSequenceListener {

          val results = Vector.newBuilder[(Key, Record)]

          override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

          override def onRecord(key: Key, record: Record): Unit = results += (key -> record)

          override def onSuccess(): Unit = promise.success(results.result())

        }, m.queryPolicy.orNull, statement.toStatement)

      promise.future
    }

    case ScanAll(ns, set, bins) => kleisli[Vector[(Key, Record)]] { m =>
      val promise = Promise[Vector[(Key, Record)]]()

        m.client.scanAll(m.eventLoops.next(), new RecordSequenceListener {

          val results = Vector.newBuilder[(Key, Record)]

          override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

          override def onRecord(key: Key, record: Record): Unit = results += (key -> record)

          override def onSuccess(): Unit = promise.success(results.result())

        }, m.scanPolicy.orNull, ns, set, bins: _*)

      promise.future
    }

    case GetAll(keys) => kleisli[Vector[(Key, Record)]] { m =>
      val promise = Promise[Vector[(Key, Record)]]()

        m.client.get(m.eventLoops.next(), new RecordSequenceListener {

          val results = Vector.newBuilder[(Key, Record)]

          override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

          override def onRecord(key: Key, record: Record): Unit = results += (key -> record)

          override def onSuccess(): Unit = promise.success(results.result())

        }, m.batchPolicy.orNull, keys.toArray)

      promise.future
    }

      //TODO not sure about what header do?
    case Header(key) => kleisli[Unit] { m =>
      val promise = Promise[Unit]

      m.client.getHeader(m.eventLoops.next(), new RecordListener {
        override def onFailure(exception: AerospikeException): Unit = promise.failure(exception)

        override def onSuccess(key: Key, record: Record): Unit = promise.success(())
      }, m.policy.orNull, key)

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