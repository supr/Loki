package com.memeo.loki

import java.net.InetSocketAddress
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
  if (args.length < 3) {
    sys.error("usage: com.memeo.loki.Main <my-id> <i> <n>")
    sys.exit(-1)
  }
  val me = Integer.parseInt(args(0))
  val i_ = Integer.parseInt(args(1))
  val n_ = Integer.parseInt(args(2))
  val peers_ = List.range(0, n_).foldLeft(Map[Int, Member]())((m, i) => {
    if (i == me)
      m ++ Map(me -> Self(me, "loki" + me))
    else
      m ++ Map(i -> Peer("akka://loki@127.0.0.1:" + (7777 + i) + "/user/loki", i, "loki" + i))
  })

  class conf extends ClusterConfig
  {
    override val i = i_
    override val n = n_
    override val peers = peers_
  }
  val system = ActorSystem("loki", ConfigFactory.parseString("akka.remote.netty.port=" + (7777 + me)).withFallback(ConfigFactory.parseFile(new File("akka.conf")).withFallback(ConfigFactory.defaultOverrides())));
  val logger = Logging(system, getClass())
  val service = system.actorOf(Props(new LokiService(new File("loki" + me), new conf())), name = "loki")
  val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
  bootstrap.setPipelineFactory(new HttpServerPipelineFactory(service, system))
  val bindAddress = new InetSocketAddress(8080 + me)
  bootstrap.bind(bindAddress)
  logger.info("Loki has started me={} i={} n={}", me, i_, n_)
  logger.info("http={}", bindAddress)
}
