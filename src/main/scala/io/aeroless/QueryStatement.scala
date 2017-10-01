package io.aeroless

import com.aerospike.client.Value
import com.aerospike.client.query.{Filter, Statement}

case class QueryStatement(
  namespace: String,
  set: String,
  bins: Option[Array[String]] = None,
  filter: Option[Filter] = None,
  fun: Option[(AggregateFunction, Seq[Value])] = None
) {

  def onRange(bin: String, start: Long, end: Long): QueryStatement = {
    copy(filter = Some(Filter.range(bin, start, end)))
  }

  def binEqualTo(bin: String, value: Long): QueryStatement = {
    copy(filter = Some(Filter.equal(bin, value)))
  }

  def binEqualTo(bin: String, value: Int): QueryStatement = {
    copy(filter = Some(Filter.equal(bin, value)))
  }

  def binEqualTo(bin: String, value: String): QueryStatement = {
    copy(filter = Some(Filter.equal(bin, value)))
  }

  def readBins(bins: String*): QueryStatement = {
    copy(bins = Some(bins.toArray))
  }

  def aggregate(fun: AggregateFunction)(values: Value*): QueryStatement = {
    copy(fun = Some(
      fun, values
    ))
  }

  def toStatement: Statement = {
    val s = new Statement()
    s.setNamespace(namespace)
    s.setSetName(set)
    filter.map(s.setFilter)
    bins.map(s.setBinNames(_: _*))
    fun.map { case (af, values) =>
      s.setAggregateFunction(af.classLoader, af.path, af.pack, af.funName, values: _*)
    }
    s
  }
}

case class AggregateFunction(path: String, pack: String, funName: String, classLoader: ClassLoader = DefaultClassLoader)
