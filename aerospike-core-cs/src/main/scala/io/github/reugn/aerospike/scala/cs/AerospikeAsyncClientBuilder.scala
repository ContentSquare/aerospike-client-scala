package io.github.reugn.aerospike.scala.cs

import com.aerospike.client.async.EventLoops
import com.aerospike.client.{AerospikeClient, Host}
import com.typesafe.config.Config

class AerospikeAsyncClientBuilder(conf: AerospikeClientConf) {

  def build(): AerospikeClient = {
    val clientPolicy = conf.clientPolicyConf.getClientPolicy
    val hostName = conf.hostname
    val port = conf.port
    val hosts = conf.hosts

    if (hosts != "") {
      new AerospikeClient(clientPolicy,
        Host.parseHosts(hosts, port): _*)
    } else {
      new AerospikeClient(clientPolicy, hostName, port)
    }
  }
}

object AerospikeAsyncClientBuilder {
  def apply(config: Config): AerospikeAsyncClientBuilder = new
      AerospikeAsyncClientBuilder(
        AerospikeClientConf(config)
      )

  def apply(aerospikeClientConf: AerospikeClientConf): AerospikeAsyncClientBuilder
  = new AerospikeAsyncClientBuilder(aerospikeClientConf)

  def createClient(config: Config): (AerospikeClient, EventLoops) = {
    val aerospikeClientConf = AerospikeClientConf(config)
    createClient(aerospikeClientConf)
  }

  def createClient(aerospikeClientConf: AerospikeClientConf): (AerospikeClient, EventLoops) = {
    val aerospikeClient = (new AerospikeAsyncClientBuilder(aerospikeClientConf)).build()
    (aerospikeClient, aerospikeClientConf.eventLoops)
  }
}
