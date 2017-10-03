package io.aeroless

import com.aerospike.client.Value
import com.aerospike.client.query.{Filter, Statement}

class QueryStatementBuilder(
  namespace: String,
  set: String,
  bins: Seq[String]
) {

  def onRange(bin: String, start: Long, end: Long): QueryStatement = new QueryStatement {
    override def toStatement: Statement = {
      val s = baseStatement()
      s.setFilter(Filter.range(bin, start, end))
      s
    }
  }

  def binEqualTo(bin: String, value: Long): QueryStatement = new QueryStatement {
    override def toStatement: Statement = {
      val s = baseStatement()
      s.setFilter(Filter.equal(bin, value))
      s
    }
  }

  def binEqualTo(bin: String, value: Int): QueryStatement = new QueryStatement {
    override def toStatement: Statement = {
      val s = baseStatement()
      s.setFilter(Filter.equal(bin, value))
      s
    }
  }

  def binEqualTo(bin: String, value: String): QueryStatement = new QueryStatement {
    override def toStatement: Statement = {
      val s = baseStatement()
      s.setFilter(Filter.equal(bin, value))
      s
    }
  }

  private def baseStatement(): Statement = {
    val s = new Statement()
    s.setNamespace(namespace)
    s.setSetName(set)
    if (bins.nonEmpty) s.setBinNames(bins: _*)
    s
  }
}

sealed trait QueryStatement { self =>
  def toStatement: Statement

  def aggregate(af: AggregateFunction)(values: Value*) = new QueryStatement {
    override def toStatement: Statement = {
      val s = self.toStatement
      s.setAggregateFunction(af.classLoader, af.path, af.pack, af.funName, values: _*)
      s
    }
  }
}

case class AggregateFunction(path: String, pack: String, funName: String, classLoader: ClassLoader = DefaultClassLoader)
