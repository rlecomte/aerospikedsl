package io.aeroless

import com.aerospike.client.async.{AsyncClient, EventLoops}

trait AerospikeManager {
  val client: AsyncClient
  val eventLoops: EventLoops
}
