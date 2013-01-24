package com.memeo.loki

import java.net.InetSocketAddress
import akka.zeromq._
import akka.actor.{Props, ActorSystem}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import java.io.File
import com.typesafe.config.ConfigFactory
import akka.event.Logging

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 1:36 PM
 * To change this template use File | Settings | File Templates.
 */
object Main extends App
{
  val me = Integer.parseInt(args(0))
  val i_ = Integer.parseInt(args(1))
  val n_ = Integer.parseInt(args(2))
  val peers_ = List.range(0, n_).foldLeft(Map[Int, Member]())((m, i) => {
    if (i == me)
      m ++ Map(me -> Self(me, "loki" + me))
    else
      m ++ Map(i -> Peer("tcp://127.0.0.1:" + (7777 + i), i, "loki" + i))
  })

  class conf extends ClusterConfig
  {
    override val i = i_
    override val n = n_
    override val peers = peers_
  }
  val system = ActorSystem("loki", ConfigFactory.parseFile(new File("akka.conf")))
  val logger = Logging(system, getClass())
  val service = system.actorOf(Props(new LokiService(new File("loki" + me), new conf())), name = "loki" + me)
  val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
  bootstrap.setPipelineFactory(new HttpServerPipelineFactory(service, system))
  val bindAddress = new InetSocketAddress(8080 + me)
  bootstrap.bind(bindAddress)
  val zBind = "tcp://127.0.0.1:" + (7777 + me)
  val reqSocket = ZeroMQExtension(system).newRepSocket(Array(Bind(zBind), Listener(system.actorOf(Props(new LokiIPCActor(service)), name="zmq-ipc" + me))))
  logger.info("Loki has started me={} i={} n={}", me, i_, n_)
  logger.info("http={} zmq={}", bindAddress, zBind)
}
