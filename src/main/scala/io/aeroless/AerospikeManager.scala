package io.aeroless

import scala.concurrent.ExecutionContext

import com.aerospike.client.async.{AsyncClient, EventLoops}

trait AerospikeManager {
  val client: AsyncClient
  val eventLoops: EventLoops
  val ec: ExecutionContext
}
