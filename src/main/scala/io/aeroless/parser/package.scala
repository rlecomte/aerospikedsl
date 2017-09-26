package io.aeroless

import cats.Applicative
import io.aeroless.parser.algebra.AsAlgebra

package object parser {

  object algebra {

    sealed trait AsAlgebra[F[_]] extends Applicative[F] {
      def get[A](field: String)(next: Dsl[A]): F[A]

      def at[A](idx: Int)(next: Dsl[A]): F[A]

      def readString: F[String]

      def readLong: F[Long]

      def readNull: F[Unit]

      def readValues[A](next: Dsl[A]): F[Seq[A]]

      def readFields[A](next: Dsl[A]): F[Map[String, A]]
    }

  }

  object yolo {

    import cats.instances.function._

    type Stack[A] = AsValue => A

    private val interpreter = new AsAlgebra[Stack] {
      override def get[A](field: String)(next: Dsl[A]): Stack[A] = {
        case o@AsObject(_) => yolo.runUnsafe(next)(o.get(field).get)
      }

      override def at[A](idx: Int)(next: Dsl[A]): Stack[A] = {
        case arr@AsArray(_) => yolo.runUnsafe(next)(arr.at(idx).get)
      }

      override def readString: Stack[String] = {
        case AsString(s) => s
      }

      override def readLong: Stack[Long] = {
        case AsLong(l) => l
      }

      override def readNull: Stack[Unit] = _ => ()

      override def readValues[A](next: Dsl[A]): Stack[Seq[A]] = {
        case AsArray(arr) => arr.map(e => yolo.runUnsafe(next).apply(e.value))
      }

      override def readFields[A](next: Dsl[A]): Stack[Map[String, A]] = {
        case AsObject(map) => map.mapValues(e => yolo.runUnsafe(next).apply(e.value))
      }

      override def pure[A](x: A): Stack[A] = Applicative[Stack].pure(x)

      override def ap[A, B](ff: Stack[(A) => B])(fa: Stack[A]): Stack[B] = Applicative[Stack].ap(ff)(fa)

    }

    def runUnsafe[A](prog: Dsl[A]): Stack[A] = prog(interpreter)
  }

  sealed trait Dsl[A] {
    def apply[F[_]](implicit ev: AsAlgebra[F]): F[A]
  }

  def get[A](path: String)(next: Dsl[A]): Dsl[A] = new Dsl[A] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[A] = ev.get(path)(next)
  }

  def at[A](idx: Int)(next: Dsl[A]): Dsl[A] = new Dsl[A] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[A] = ev.at(idx)(next)
  }

  def readString: Dsl[String] = new Dsl[String] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[String] = ev.readString
  }

  def readLong: Dsl[Long] = new Dsl[Long] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[Long] = ev.readLong
  }

  def readNull: Dsl[Unit] = new Dsl[Unit] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[Unit] = ev.readNull
  }

  def readValues[A](next: Dsl[A]): Dsl[Seq[A]] = new Dsl[Seq[A]] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[Seq[A]] = ev.readValues(next)
  }

  def readFields[A](next: Dsl[A]): Dsl[Map[String, A]] = new Dsl[Map[String, A]] {
    override def apply[F[_]](implicit ev: AsAlgebra[F]): F[Map[String, A]] = ev.readFields(next)
  }

  implicit val dslApplicative: Applicative[Dsl] = new Applicative[Dsl] {
    override def pure[A](x: A): Dsl[A] = new Dsl[A] {
      override def apply[F[_]](implicit ev: AsAlgebra[F]): F[A] = ev.pure(x)
    }

    override def ap[A, B](ff: Dsl[(A) => B])(fa: Dsl[A]): Dsl[B] = new Dsl[B] {

      override def apply[F[_]](implicit ev: AsAlgebra[F]): F[B] = ev.ap(ff.apply[F])(fa.apply[F])
    }
  }
}
