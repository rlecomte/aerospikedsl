package io.aeroless

import com.aerospike.client.{Bin, Value}
import com.aerospike.client.Value._
import com.aerospike.client.command.ParticleType

trait Encoder[A] {
  self =>

  def encode(a: A): Seq[Bin]

  def contramap[B](f: B => A): Encoder[B] = (b: B) => self.encode(f(b))
}

object Encoder {

  import shapeless._
  import shapeless.labelled._
  import shapeless.ops.hlist.IsHCons
  import scala.collection.JavaConverters._

  trait InternalEncoder[A] {
    def encode(a: A): Value
  }

  private def instance[A](f: A => Value) = new InternalEncoder[A] {
    override def encode(a: A): Value = f(a)
  }

  implicit val longEncoder: InternalEncoder[Long] = instance(v => new LongValue(v))

  implicit val stringEncoder: InternalEncoder[String] = instance(v => new StringValue(v))

  implicit def mapEncoder[V](implicit evV: InternalEncoder[V]): InternalEncoder[Map[String, V]] = instance(kv => new MapValue(kv.mapValues(evV.encode).asJava))

  implicit def seqEncoder[A](implicit ev: InternalEncoder[A]): InternalEncoder[Seq[A]] = instance { list =>
    new ListValue(list.map(v => ev.encode(v)).asJava)
  }

  implicit def optionEncoder[A](implicit ev: InternalEncoder[A]): InternalEncoder[Option[A]] = instance {
    case Some(a) => ev.encode(a)
    case None => NullValue.INSTANCE
  }

  implicit val hnilEncoder: InternalEncoder[HNil] = instance(_ => new MapValue(new java.util.HashMap[String, AnyRef]()))

  implicit def hlistEncoder[K <: Symbol, H, T <: shapeless.HList](
    implicit witness: Witness.Aux[K],
    isHCons: IsHCons.Aux[H :: T, H, T],
    hEncoder: Lazy[InternalEncoder[H]],
    tEncoder: Lazy[InternalEncoder[T]]
  ): InternalEncoder[FieldType[K, H] :: T] = instance { o =>

    /*
      dirty mutable code below. we keep reference to the same MapValue with a mutable HashMap inside.
      Recursively fill it with (field -> value) pair.
     */
    val headValue = hEncoder.value.encode(isHCons.head(o))
    val mapValue = tEncoder.value.encode(isHCons.tail(o))
    val map = mapValue.getObject.asInstanceOf[java.util.Map[String, AnyRef]]
    map.put(witness.value.name, headValue)
    mapValue
  }

  implicit def objectEncoder[A, Repr <: HList](
    implicit gen: LabelledGeneric.Aux[A, Repr],
    hlistEncoder: InternalEncoder[Repr]
  ): Encoder[A] = (o: A) => {
    val value = hlistEncoder.encode(gen.to(o))

    value.getType() match {
      case ParticleType.MAP => {
        val map = value.getObject.asInstanceOf[java.util.Map[String, AnyRef]]
        map.asScala.map { case (k, v) => new Bin(k, v) }.toSeq
      }
      case _ => new Bin("value", value) :: Nil
    }
  }

  def apply[A](implicit ev: Encoder[A]): Encoder[A] = ev
}



