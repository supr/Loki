package com.memeo.loki

import akka.actor.{Props, ActorSystem}
import java.io.File
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import scala.Array

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
object TinyCluster
{
  /*val config1 = new ClusterConfig {
    val n: Int = 3
    val i: Int = 2
    val peers: Map[Int, Member] = Map(0 -> Self(0, "loki1"),
                                      1 -> Peer("tcp://127.0.0.1:7778", 1, "loki2"),
                                      2 -> Peer("tcp://127.0.0.1:7779", 2, "loki3"))
  }
  val system1 = ActorSystem("loki1")
  val service1 = system1.actorOf(Props(new LokiService(new File("loki1"), config1)), name = "loki1")
  val bootstrap1 = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
  bootstrap1.setPipelineFactory(new HttpServerPipelineFactory(service1, system1))
  bootstrap1.bind(new InetSocketAddress(8080))
  val reqSocket1 = ZeroMQExtension(system1).newRepSocket(Array(Bind("tcp://127.0.0.1:7777"), Listener(system1.actorOf(Props(new LokiIPCActor(service1)), name="zmq-ipc1"))))

  val config2 = new ClusterConfig {
    val n: Int = 3
    val i: Int = 2
    val peers: Map[Int, Member] = Map(0 -> Peer("tcp://127.0.0.1:7777", 0, "loki1"),
                                      1 -> Self(1, "loki2"),
                                      2 -> Peer("tcp://127.0.0.1:7779", 2, "loki3"))
  }
  val system2 = ActorSystem("loki2")
  val service2 = system2.actorOf(Props(new LokiService(new File("loki2"), config2)), name = "loki2")
  val bootstrap2 = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
  bootstrap2.setPipelineFactory(new HttpServerPipelineFactory(service2, system2))
  bootstrap2.bind(new InetSocketAddress(8081))
  val reqSocket2 = ZeroMQExtension(system2).newRepSocket(Array(Bind("tcp://127.0.0.1:7778"), Listener(system2.actorOf(Props(new LokiIPCActor(service2)), name="zmq-ipc2"))))

  val config3 = new ClusterConfig {
    val n: Int = 3
    val i: Int = 2
    val peers: Map[Int, Member] = Map(0 -> Peer("tcp://127.0.0.1:7777", 0, "loki1"),
                                      1 -> Peer("tcp://127.0.0.1:7778", 1, "loki2"),
                                      2 -> Self(2, "loki3"))
  }
  val system3 = ActorSystem("loki3")
  val service3 = system3.actorOf(Props(new LokiService(new File("loki3"), config3)), name = "loki3")
  val bootstrap3 = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
  bootstrap3.setPipelineFactory(new HttpServerPipelineFactory(service3, system3))
  bootstrap3.bind(new InetSocketAddress(8082))
  val reqSocket3 = ZeroMQExtension(system3).newRepSocket(Array(Bind("tcp://127.0.0.1:7779"), Listener(system3.actorOf(Props(new LokiIPCActor(service3)), name="zmq-ipc3"))))*/
}
