package io.github.reugn.aerospike.scala.cs

import com.aerospike.client.async.{NettyEventLoops, NioEventLoops}
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService, ForAllTestContainer}
import com.typesafe.config.ConfigFactory
import io.github.reugn.aerospike.scala.cs.util.Docker.{AerospikeHost, AerospikePort}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import pureconfig.generic.ProductHint
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource}

import java.io.File

class AerospikeAsyncClientBuilderTest
  extends AsyncFlatSpec
    with Matchers
    with ForAllTestContainer
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override val container = new DockerComposeContainer(
    new File(getClass.getClassLoader.getResource("docker-compose.yml").toURI),
    Seq(ExposedService(AerospikeHost, AerospikePort))
  )

  override def beforeAll(): Unit =
    container.start()

  it should "load config and create event loop with type safe config" in {
    val resource = getClass.getClassLoader.getResource("reference.conf")
    val myCfg = ConfigFactory.parseFile(new File(resource.toURI))
    val asConf = AerospikeClientConf(myCfg)
    asConf.clientPolicy.maxConnsPerNode shouldBe 300
    asConf.clientPolicy.eventLoops shouldBe a[NioEventLoops]
  }

  it should "load config and create event loop with pure config" in {
    implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

    val configSource = ConfigSource.resources("reference.conf")
    val asConf = AerospikeClientConf(configSource)
    asConf.clientPolicy.maxConnsPerNode shouldBe 300
    asConf.clientPolicy.eventLoops shouldBe a[NioEventLoops]
    asConf.clientPolicy.password shouldBe "password"
  }
}
