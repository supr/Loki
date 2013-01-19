package com.memeo.loki

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import akka.zeromq._
import akka.actor.{Props, ActorSystem}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 1:36 PM
 * To change this template use File | Settings | File Templates.
 */
object Main
{
  val httpAdapter = new HttpAdapterFilter
  val service = new LokiService
  val httpServer = ServerBuilder().codec(Http()).bindTo(new InetSocketAddress(8080)).name("httpserver").build(httpAdapter andThen service)
  val actorSystem = ActorSystem("zeromq")
  val reqSocket = ZeroMQExtension(actorSystem).newRepSocket(Array(Bind("tcp://0.0.0.0:7777"), Listener(actorSystem.actorOf(Props(new LokiIPCActor(service))))))
}
