package io

import scala.util.Try

import com.aerospike.client.{Key, Record}

import io.aeroless.connection.AerospikeIO
import io.aeroless.parser.{AsValue, Decoder, Dsl, yolo}

package object aeroless {

  object keydomain {
    trait KeyBuilder {
      def apply(idx: String): Key
      def apply(idx: Long): Key
      def apply(idx: Int): Key
    }

    def apply(namespace: String, set: String): KeyBuilder = new KeyBuilder {
      override def apply(idx: String): Key = new Key(namespace, set, idx)

      override def apply(idx: Long): Key = new Key(namespace, set, idx)

      override def apply(idx: Int): Key = new Key(namespace, set, idx)
    }
  }

  implicit class DslOps[A](dsl: Dsl[A]) {
    def runUnsafe(value: AsValue): Either[Throwable, A] = Try {
      yolo.runUnsafe(dsl)(value)
    }.toEither
  }

  implicit class AerospikeIOOps(io: AerospikeIO[Vector[(Key, Record)]]) {

    def decodeOrFail[T](implicit ev: Decoder[T]): AerospikeIO[Vector[(Key, T)]] = {
      io.flatMap[Vector[(Key, T)]] { vector =>

        import cats.instances.either._
        import cats.instances.vector._
        import cats.syntax.traverse._

        vector.traverse[Either[Throwable, ?], (Key, T)] { t =>
          ev.dsl.runUnsafe(AsValue.fromRecord(t._2)).map(r => (t._1, r))
        } match {
          case Right(vec) => AerospikeIO.pure(vec)
          case Left(t) => AerospikeIO.failed(t)
        }
      }
    }
  }
}