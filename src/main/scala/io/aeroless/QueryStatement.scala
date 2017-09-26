package io.aeroless

import com.aerospike.client.query.{Filter, Statement}

case class QueryStatement(
  namespace: String,
  set: String,
  bins: Option[Array[String]] = None,
  filter: Option[Filter] = None
) {

  def onRange(bin: String, start: Long, end: Long): QueryStatement ={
    copy(filter = Some(Filter.range(bin, start, end)))
  }

  def readBins(bins: String*): QueryStatement = {
    copy(bins = Some(bins.toArray))
  }

  def toStatement: Statement = {
    val s = new Statement()
    s.setNamespace(namespace)
    s.setSetName(set)
    filter.map(s.setFilter)
    bins.map(s.setBinNames(_: _*))
    s
  }
}
