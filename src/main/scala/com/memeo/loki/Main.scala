package com.memeo.loki

import java.net.InetSocketAddress
import akka.zeromq._
import akka.actor.{Props, ActorSystem}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 1:36 PM
 * To change this template use File | Settings | File Templates.
 */
object Main
{
  val system = ActorSystem("loki")
  val service = system.actorOf(Props(new LokiService(new File("loki"))), name = "loki")
  val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
  bootstrap.setPipelineFactory(new HttpServerPipelineFactory(service, system))
  bootstrap.bind(new InetSocketAddress(8080))
  val reqSocket = ZeroMQExtension(system).newRepSocket(Array(Bind("tcp://0.0.0.0:7777"), Listener(system.actorOf(Props(new LokiIPCActor(service)), name="zmq-ipc"))))
}
