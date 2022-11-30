package io.github.reugn.aerospike.scala.cs

import com.aerospike.client.async.{EventLoopType, EventPolicy}
import com.aerospike.client.policy.{AuthMode, ClientPolicy}
import com.typesafe.config.Config
import pureconfig.{ConfigObjectSource, ConfigSource, ConfigReader, loadConfig, CamelCase, ConfigFieldMapping}
import pureconfig.generic.auto._
import pureconfig.generic.{ProductHint}


final case class EventLoopConf(
                                threadNumbers: Int,
                                eventLoopType: EventLoopType,
                                maxCommandsInProcess: Int,
                                maxCommandsInQueue: Int,
                                queueInitialCapacity: Int,
                                minTimeout: Int,
                                ticksPerWheel: Int,
                                commandsPerEventLoop: Int
                              ) {
  lazy val eventPolicy: EventPolicy = getEventPolicy

  def getEventPolicy: EventPolicy = {
    val policy = new EventPolicy()
    policy.maxCommandsInProcess = maxCommandsInProcess
    policy.maxCommandsInQueue = maxCommandsInQueue
    policy.queueInitialCapacity = queueInitialCapacity
    policy.minTimeout = minTimeout
    policy.ticksPerWheel = ticksPerWheel
    policy.commandsPerEventLoop = commandsPerEventLoop
    policy
  }
}

object EventLoopConf {
  val defaultThreadNumbers = 1
  val defaultEventLoopType = EventLoopType.DIRECT_NIO

  def defaultEventLoopConf() = {
    val policy = new EventPolicy
    EventLoopConf(defaultThreadNumbers, defaultEventLoopType,
      policy.maxCommandsInProcess, policy.maxCommandsInQueue, policy.queueInitialCapacity,
      policy.minTimeout, policy.ticksPerWheel, policy.commandsPerEventLoop
    )
  }
}


final case class ClientPolicyConf(
                                   eventLoop: EventLoopConf,
                                   user: String,
                                   password: String,
                                   clusterName: String,
                                   authMode: AuthMode,
                                   timeout: Int,
                                   loginTimeout: Int,
                                   minConnsPerNode: Int,
                                   maxConnsPerNode: Int,
                                   asyncMinConnsPerNode: Int,
                                   asyncMaxConnsPerNode: Int,
                                   connPoolsPerNode: Int,
                                   maxSocketIdle: Int,
                                   maxErrorRate: Int,
                                   errorRateWindow: Int,
                                   tendInterval: Int,
                                   failIfNotConnected: Boolean
                                 ) {

  lazy val eventLoops = EventLoopsBuilder.createEventLoops(eventLoop)
  lazy val clientPolicy = getClientPolicy

  def getClientPolicy: ClientPolicy = {
    val policy = new ClientPolicy()
    policy.eventLoops = eventLoops
    policy.user = user
    policy.password = password
    policy.authMode = authMode
    policy.timeout = timeout
    policy.loginTimeout = loginTimeout
    policy.minConnsPerNode = minConnsPerNode
    policy.maxConnsPerNode = maxConnsPerNode
    policy.asyncMinConnsPerNode = asyncMinConnsPerNode
    policy.asyncMaxConnsPerNode = asyncMaxConnsPerNode
    policy.connPoolsPerNode = connPoolsPerNode
    policy.maxSocketIdle = maxSocketIdle
    policy.maxErrorRate = maxErrorRate
    policy.errorRateWindow = errorRateWindow
    policy.tendInterval = tendInterval
    policy.failIfNotConnected = failIfNotConnected
    policy
  }
}

object ClientPolicyConf {
  def defaultClientPolicyConf() = {
    val policy = new ClientPolicy()
    ClientPolicyConf(EventLoopConf.defaultEventLoopConf(),
      policy.user,
      policy.password,
      policy.clusterName,
      policy.authMode,
      policy.timeout,
      policy.loginTimeout,
      policy.minConnsPerNode,
      policy.maxConnsPerNode,
      policy.asyncMinConnsPerNode,
      policy.asyncMaxConnsPerNode,
      policy.connPoolsPerNode,
      policy.maxSocketIdle,
      policy.maxErrorRate,
      policy.errorRateWindow,
      policy.tendInterval,
      policy.failIfNotConnected
    )
  }
}

final case class AerospikeClientConf(
                                      hosts: String,
                                      hostname: String,
                                      port: Int,
                                      clientPolicy: ClientPolicyConf
                                    ) {
  val eventLoops = clientPolicy.eventLoops
}

object AerospikeClientConf {
  val defaultHostName = "localhost"
  val defaultPort = 3000
  val defaultHosts = ""

  def apply(config: Config): AerospikeClientConf = {
    loadWithSource(ConfigSource.fromConfig(config))
  }

  def apply(source: ConfigObjectSource) : AerospikeClientConf = {
    loadWithSource(source)
  }

  def loadWithSource(source: ConfigObjectSource): AerospikeClientConf = {
    implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

    val defaultSource: ConfigObjectSource = ConfigSource.resources("reference-default.conf")
   source.withFallback(defaultSource).at("aerospike")
      .loadOrThrow[AerospikeClientConf]
  }

  def defaultAerospikeClientConf(): AerospikeClientConf = {
    AerospikeClientConf(defaultHosts, defaultHostName, defaultPort, ClientPolicyConf.defaultClientPolicyConf())
  }
}
