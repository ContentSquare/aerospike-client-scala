package io.github.reugn.aerospike.scala.cs

import com.aerospike.client.async.{EventLoopType, EventPolicy}
import com.aerospike.client.policy.{AuthMode, ClientPolicy}
import com.typesafe.config.Config


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
                                   eventLoopConf: EventLoopConf,
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

  lazy val eventLoops = EventLoopsBuilder.createEventLoops(eventLoopConf)
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
                                      clientPolicyConf: ClientPolicyConf
                                    ) {
  val eventLoops = clientPolicyConf.eventLoops
}

object AerospikeClientConf {
  val defaultHostName = "localhost"
  val defaultPort = 3000
  val defaultHosts = ""

  def apply(config: Config): AerospikeClientConf = {
    val defaultEventLoopConf = EventLoopConf.defaultEventLoopConf()
    val defaultClientPolicyConf = ClientPolicyConf.defaultClientPolicyConf()
    val defaultAerospikeClientConf = AerospikeClientConf.defaultAerospikeClientConf()

    val aerospikeConfig = config.getConfig("aerospike")
    val clientPolicyConfig = aerospikeConfig.getConfig("clientpolicy")
    val eventLoopConfig = clientPolicyConfig.getConfig("eventloop")
    val eventLoopConf = EventLoopConf(
      getEventLoopThreads(getIntParam(eventLoopConfig, "threadNumbers").getOrElse(defaultEventLoopConf.threadNumbers)),
      EventLoopType.valueOf(
        getStringParam(eventLoopConfig, "eventLoopType").getOrElse(defaultEventLoopConf.eventLoopType.toString)),
      getIntParam(
        eventLoopConfig, "maxCommandsInProcess"
      ).getOrElse(defaultEventLoopConf.maxCommandsInProcess),
      getIntParam(
        eventLoopConfig, "maxCommandsInQueue"
      ).getOrElse(defaultEventLoopConf.maxCommandsInQueue),
      getIntParam(
        eventLoopConfig, "queueInitialCapacity"
      ).getOrElse(defaultEventLoopConf.queueInitialCapacity),
      getIntParam(
        eventLoopConfig, "minTimeout"
      ).getOrElse(defaultEventLoopConf.minTimeout),
      getIntParam(
        eventLoopConfig,
        "ticksPerWheel").getOrElse(defaultEventLoopConf.ticksPerWheel),
      getIntParam(
        eventLoopConfig,
        "commandsPerEventLoop").getOrElse(defaultEventLoopConf.commandsPerEventLoop)
    )

    val clientPolicyConf = ClientPolicyConf(
      eventLoopConf,
      getStringParam(clientPolicyConfig,
        "user").getOrElse(defaultClientPolicyConf.user),
      getStringParam(clientPolicyConfig, "password").getOrElse(defaultClientPolicyConf.password),
      getStringParam(clientPolicyConfig, "clusterName").getOrElse(defaultClientPolicyConf.clusterName),
      AuthMode.valueOf(getStringParam(clientPolicyConfig, "authMode").getOrElse(defaultClientPolicyConf.authMode.toString)),
      getIntParam(clientPolicyConfig, "timeout").getOrElse(defaultClientPolicyConf.timeout),
      getIntParam(clientPolicyConfig, "loginTimeout").getOrElse(defaultClientPolicyConf.loginTimeout),
      getIntParam(clientPolicyConfig, "minConnsPerNode").getOrElse(defaultClientPolicyConf.minConnsPerNode),
      getIntParam(clientPolicyConfig, "maxConnsPerNode").getOrElse(defaultClientPolicyConf.maxConnsPerNode),
      getIntParam(clientPolicyConfig, "asyncMinConnsPerNode").getOrElse(defaultClientPolicyConf.asyncMinConnsPerNode),
      getIntParam(clientPolicyConfig, "asyncMaxConnsPerNode").getOrElse(defaultClientPolicyConf.asyncMaxConnsPerNode),
      getIntParam(clientPolicyConfig, "connPoolsPerNode").getOrElse(defaultClientPolicyConf.connPoolsPerNode),
      getIntParam(clientPolicyConfig, "maxSocketIdle").getOrElse(defaultClientPolicyConf.maxSocketIdle),
      getIntParam(clientPolicyConfig, "maxErrorRate").getOrElse(defaultClientPolicyConf.maxErrorRate),
      getIntParam(clientPolicyConfig, "errorRateWindow").getOrElse(defaultClientPolicyConf.errorRateWindow),
      getIntParam(clientPolicyConfig, "tendInterval").getOrElse(defaultClientPolicyConf.tendInterval),
      getBoolean(clientPolicyConfig, "failIfNotConnected").getOrElse(defaultClientPolicyConf.failIfNotConnected)
    )

    AerospikeClientConf(
      getStringParam(aerospikeConfig, "hostList").getOrElse(defaultAerospikeClientConf.hosts),
      getStringParam(aerospikeConfig, "hostname").getOrElse(defaultAerospikeClientConf.hostname),
      getIntParam(aerospikeConfig, "port").getOrElse(defaultAerospikeClientConf.port),
      clientPolicyConf
    )
  }

  def defaultAerospikeClientConf(): AerospikeClientConf = {
    AerospikeClientConf(defaultHosts, defaultHostName, defaultPort, ClientPolicyConf.defaultClientPolicyConf())
  }

  private def getIntParam(config: Config, key: String): Option[Int] = {
    try {
      Option(config.getInt(key))
    } catch {
      case _: Throwable => None
    }
  }

  private def getStringParam(config: Config, key: String): Option[String] = {
    try {
      Option(config.getString(key))
    } catch {
      case _: Throwable => None
    }
  }

  private def getBoolean(config: Config, key: String): Option[Boolean] = {
    try {
      Option(config.getBoolean(key))
    } catch {
      case _: Throwable => None
    }
  }

  private def getEventLoopThreads(threadNumbers: Int): Int = {
    val max = Runtime.getRuntime.availableProcessors
    if (threadNumbers > 0 && threadNumbers <= max) threadNumbers
    else max
  }
}
