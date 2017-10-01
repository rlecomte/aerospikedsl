package io

import scala.concurrent.{ExecutionContext, Future}

import com.aerospike.client.query.IndexType
import com.aerospike.client.{Bin, Key, Operation, Record}

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

  object ops {
    def prepend(binName: String, value: String): Operation = Operation.prepend(new Bin(binName, value))

    def append(binName: String, value: String): Operation = Operation.append(new Bin(binName, value))

    def put(binName: String, value: String): Operation = Operation.put(new Bin(binName, value))

    def put(binName: String, value: Long): Operation = Operation.put(new Bin(binName, value))

    def put(binName: String, value: Int): Operation = Operation.put(new Bin(binName, value))

    def add(binName: String, value: Long): Operation = Operation.add(new Bin(binName, value))

    def add(binName: String, value: Int): Operation = Operation.add(new Bin(binName, value))

    def get(binName: String): Operation = Operation.get(binName)

    def getAll: Operation = Operation.get()

    def touch: Operation = Operation.touch()
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

    def append(key: Key, bins: Map[String, String]): AerospikeIO[Key] = {
      Append(key, bins.map { case (k, v) => new Bin(k, v) }.toSeq)
    }

    def prepend(key: Key, bins: Map[String, String]): AerospikeIO[Key] = {
      Prepend(key, bins.map { case (k, v) => new Bin(k, v) }.toSeq)
    }

    def delete(key: Key): AerospikeIO[Key] = {
      Delete(key)
    }

    def add(key: Key, numBin: Seq[(String, Long)]): AerospikeIO[Key] = {
      Add(key, numBin.map { case (k, v) => new Bin(k, v) })
    }

    def get[T](key: Key, bins: Seq[String])(implicit decoder: Decoder[T]): AerospikeIO[Option[T]] = {
      Get(key, bins).flatMap {
        case Some(r) =>
          decoder.dsl.runEither(AsValue.fromRecord(r)) match {
            case Right(v) => AerospikeIO.successful(Some(v))
            case Left(err) => AerospikeIO.failed(err)
          }
        case None => AerospikeIO.successful(None)
      }
    }

    def touch(key: Key): AerospikeIO[Key] = {
      Touch(key)
    }

    def header(key: Key): AerospikeIO[Unit] = {
      Header(key)
    }

    def exists(key: Key): AerospikeIO[Boolean] = {
      Exists(key)
    }

    def query[T](statement: QueryStatement)(implicit decoder: Decoder[T]): AerospikeIO[Vector[(Key, T)]] = {
      Query(statement).flatMap(decodeVector[T])
    }

    def scanAll[T](namespace: String, set: String, binNames: Seq[String])
      (implicit decoder: Decoder[T]): AerospikeIO[Vector[(Key, T)]] = {
      ScanAll(namespace, set, binNames).flatMap(decodeVector[T])
    }

    def getAll[T](keys: Seq[Key])(implicit decoder: Decoder[T]): AerospikeIO[Vector[(Key, T)]] = {
      GetAll(keys).flatMap(decodeVector[T])
    }

    def createIndex(namespace: String, set: String, binName: String, idxType: IndexType, idx: Option[String] = None): AerospikeIO[String] = {
      CreateIndex(namespace, set, binName, idxType, idx)
    }

    def dropIndex(namespace: String, set: String, index: String): AerospikeIO[Unit] = {
      DropIndex(namespace, set, index)
    }

    def operate[T](key: Key)(ops: Operation*)(implicit decoder: Decoder[T]): AerospikeIO[Option[T]] = {
      Operate(key, ops).flatMap {
        case Some(r) =>
          decoder.dsl.runEither(AsValue.fromRecord(r)) match {
            case Right(v) => AerospikeIO.successful(Some(v))
            case Left(err) => AerospikeIO.failed(err)
          }
        case None => AerospikeIO.successful(None)
      }
    }

    private def decodeVector[T](vector: Vector[(Key, Record)])
      (implicit decoder: Decoder[T]): AerospikeIO[Vector[(Key, T)]] = {
      import cats.instances.either._
      import cats.instances.vector._
      import cats.syntax.traverse._

      vector.traverse[Either[Throwable, ?], (Key, T)] { t =>
        decoder.dsl.runEither(AsValue.fromRecord(t._2)).map(r => (t._1, r))
      } match {
        case Right(vec) => AerospikeIO.successful(vec)
        case Left(t) => AerospikeIO.failed(t)
      }
    }
  }

  implicit class AerospikeIOOps[A](io: AerospikeIO[A]) {

    def runFuture(manager: AerospikeManager)(implicit ec: ExecutionContext): Future[A] = {
      KleisliInterpreter.apply(ec)(io).apply(manager)
    }
  }

}