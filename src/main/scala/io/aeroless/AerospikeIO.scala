package io.aeroless

import com.aerospike.client.{Bin, Key, Record}

import cats.MonadError
import io.aeroless.AerospikeIO.{Bind, FMap, Fail, Join}

abstract class AerospikeIO[A] { self =>
  def map[B](f: A => B): AerospikeIO[B] = FMap(this, f)

  def flatMap[B](f: A => AerospikeIO[B]): AerospikeIO[B] = Bind(this, f)

  def product[B](opsB: AerospikeIO[B]): AerospikeIO[(A, B)] = Join(this, opsB)

  def recover(f: Throwable => AerospikeIO[A]): AerospikeIO[A] = self match {
    case Fail(t) => f(t)
    case _ => self
  }
}

object AerospikeIO {

  implicit val monadAerospikeIO: MonadError[AerospikeIO, Throwable] = new MonadError[AerospikeIO, Throwable] {
    override def pure[A](x: A): AerospikeIO[A] = pure(x)

    override def flatMap[A, B](fa: AerospikeIO[A])(f: (A) => AerospikeIO[B]) = fa.flatMap(f)

    override def map[A, B](opA: AerospikeIO[A])(f: A => B): AerospikeIO[B] = opA.map(f)

    override def product[A, B](opA: AerospikeIO[A], opB: AerospikeIO[B]): AerospikeIO[(A, B)] = opA.product(opB)

    override def tailRecM[A, B](a: A)(f: (A) => AerospikeIO[Either[A, B]]): AerospikeIO[B] = f(a).flatMap {
      case Left(a) => tailRecM(a)(f)
      case Right(b) => Pure(b)
    }

    override def raiseError[A](e: Throwable): AerospikeIO[A] = failed(e)

    override def handleErrorWith[A](fa: AerospikeIO[A])(f: (Throwable) => AerospikeIO[A]): AerospikeIO[A] = fa.recover(f)
  }

  def successful[A](x: A): AerospikeIO[A] = Pure(x)

  def failed[A](t: Throwable): AerospikeIO[A] = Fail(t)

  //Basic
  final case class Put(key: Key, bins: Seq[Bin]) extends AerospikeIO[Key]

  final case class Append(key: Key, bins: Seq[Bin]) extends AerospikeIO[Key]

  final case class Prepend(key: Key, bins: Seq[Bin]) extends AerospikeIO[Key]

  final case class Add(key: Key, bins: Seq[Bin]) extends AerospikeIO[Key]

  final case class Get(key: Key, bins: Seq[String]) extends AerospikeIO[Option[Record]]

  final case class Delete(key: Key) extends AerospikeIO[Key]

  final case class Touch(key: Key) extends AerospikeIO[Key]

  final case class Header(key: Key) extends AerospikeIO[Unit]

  final case class Exists(key: Key) extends AerospikeIO[Boolean]

  final case class Query(statement: QueryStatement) extends AerospikeIO[Vector[(Key, Record)]]

  final case class ScanAll(namespace: String, set: String, binNames: Seq[String]) extends AerospikeIO[Vector[(Key, Record)]]

  final case class GetAll[A](keys: Seq[Key]) extends AerospikeIO[Vector[(Key, Record)]]

  final case class Pure[A, B](x: A) extends AerospikeIO[A]

  final case class Join[A, B](opA: AerospikeIO[A], opB: AerospikeIO[B]) extends AerospikeIO[(A, B)]

  final case class Bind[A, B](opA: AerospikeIO[A], f: A => AerospikeIO[B]) extends AerospikeIO[B]

  final case class FMap[A, B](opA: AerospikeIO[A], f: A => B) extends AerospikeIO[B]

  final case class Fail[A](t: Throwable) extends AerospikeIO[A]

}
