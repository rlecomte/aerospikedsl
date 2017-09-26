package io.aeroless

import com.aerospike.client.{Bin, Key, Record}

import cats.MonadError
import io.aeroless.AerospikeIO.{Bind, FMap, Join}

sealed trait AerospikeIO[A] {
  self =>

  def map[B](f: A => B): AerospikeIO[B] = FMap(self, f)

  def flatMap[B](f: A => AerospikeIO[B]): AerospikeIO[B] = Bind(self, f)

  def product[B](opsB: AerospikeIO[B]): AerospikeIO[(A, B)] = Join(self, opsB)
}

object AerospikeIO {

  implicit val monadAerospikeIO: MonadError[AerospikeIO, Throwable] = new MonadError[AerospikeIO, Throwable] {
    override def pure[A](x: A): AerospikeIO[A] = pure(x)

    override def flatMap[A, B](fa: AerospikeIO[A])(f: (A) => AerospikeIO[B]) = fa.flatMap(f)

    override def map[A, B](opA: AerospikeIO[A])(f: A => B): AerospikeIO[B] = opA.map(f)

    override def product[A, B](opA: AerospikeIO[A], opB: AerospikeIO[B]): AerospikeIO[(A, B)] = opA.product(opB)

    override def tailRecM[A, B](a: A)(f: (A) => AerospikeIO[Either[A, B]]): AerospikeIO[B] = ??? //TODO

    override def raiseError[A](e: Throwable): AerospikeIO[A] = failed(e)

    override def handleErrorWith[A](fa: AerospikeIO[A])(f: (Throwable) => AerospikeIO[A]): AerospikeIO[A] = fa match {
      case Fail(t) => f(t)
      case _ => fa
    }
  }

  def pure[A](x: A): AerospikeIO[A] = Pure(x)

  def failed[A](t: Throwable): AerospikeIO[A] = Fail(t)

  final case class Put(key: Key, bins: Seq[Bin]) extends AerospikeIO[Key]

  final case class Query(statement: QueryStatement) extends AerospikeIO[Vector[(Key, Record)]]

  final case class ScanAll(namespace: String, set: String, binNames: List[String]) extends AerospikeIO[Vector[(Key, Record)]]

  final case class GetAll[A](keys: Array[Key]) extends AerospikeIO[Vector[(Key, Record)]]

  final case class Pure[A, B](x: A) extends AerospikeIO[A]

  final case class Join[A, B](opA: AerospikeIO[A], opB: AerospikeIO[B]) extends AerospikeIO[(A, B)]

  final case class Bind[A, B](opA: AerospikeIO[A], f: A => AerospikeIO[B]) extends AerospikeIO[B]

  final case class FMap[A, B](opA: AerospikeIO[A], f: A => B) extends AerospikeIO[B]

  final case class Fail[A](t: Throwable) extends AerospikeIO[A]

}
