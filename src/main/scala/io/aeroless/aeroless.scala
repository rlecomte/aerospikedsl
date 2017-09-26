package io

import com.aerospike.client.{Key, Record}

import io.aeroless.parser.{AsValue, Decoder, Encoder}

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

  object statement {

    def apply(namespace: String, set: String): QueryStatement = {
      QueryStatement(
        namespace = namespace,
        set = set
      )
    }
  }

  object connection {

    import AerospikeIO._

    def pure[A](x: A): AerospikeIO[A] = Pure(x)

    def put[T](key: Key, obj: T)(implicit encoder: Encoder[T]): AerospikeIO[Key] = Put(key, encoder.encode(obj))

    def query[T](statement: QueryStatement)(implicit decoder: Decoder[T]): AerospikeIO[Vector[(Key, T)]] = {
      Query(statement).flatMap[Vector[(Key, T)]] { vector =>

        import cats.instances.either._
        import cats.instances.vector._
        import cats.syntax.traverse._

        vector.traverse[Either[Throwable, ?], (Key, T)] { t =>
          decoder.dsl.runEither(AsValue.fromRecord(t._2)).map(r => (t._1, r))
        } match {
          case Right(vec) => AerospikeIO.pure(vec)
          case Left(t) => AerospikeIO.failed(t)
        }
      }
    }

    def scanAll(namespace: String, set: String, binNames: List[String]): AerospikeIO[Vector[(Key, Record)]] = ScanAll(namespace, set, binNames)

    def getAll[A](keys: Array[Key]): AerospikeIO[Vector[(Key, Record)]] = GetAll(keys)
  }

}