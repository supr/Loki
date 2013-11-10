/*
 * Copyright 2013 Memeo, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.memeo.loki

import scala.concurrent.duration._
import akka.pattern.ask
import akka.actor.{Props, ActorSystem}
import java.io.File
import com.typesafe.config.ConfigFactory
import akka.event.Logging
import org.glassfish.grizzly.http.server.HttpServer
import akka.util.Timeout

object Main extends App
{
  val useJson = false

  if (args.length < 3) {
    sys.error("usage: com.memeo.loki.Main <my-id> <i> <n>")
    sys.exit(-1)
  }
  val me = Integer.parseInt(args(0))
  val i_ = Integer.parseInt(args(1))
  val n_ = Integer.parseInt(args(2))
  if (i_ < 0)
    throw new IllegalArgumentException("i must be at least 0")
  if (n_ >= (1 << i_))
    throw new IllegalArgumentException("n must be no larger than 2^i")
  val peers_ = List.range(0, (1 << i_) + n_).foldLeft(Map[Int, Member]())((m, i) => {
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
  implicit val system = ActorSystem("loki", ConfigFactory.parseString("akka.remote.netty.port=" + (7777 + me)).withFallback(ConfigFactory.parseFile(new File("akka.conf")).withFallback(ConfigFactory.defaultOverrides())));
  val logger = Logging(system, getClass)
  val service = system.actorOf(Props(new LokiService(new File("loki" + me), new conf())), "loki")
  import scala.concurrent.ExecutionContext.Implicits.global
  val httpServer = HttpServer.createSimpleServer(null, 8080 + me)
  val grizzlyActor = system.actorOf(Props(new GrizzlyAdapter(service)), "grizzly")
  implicit val timeout = Timeout(1 minute)
  for {
    result <- grizzlyActor ? httpServer
  } yield {
    //httpServer.getServerConfiguration.addHttpHandler(grizzlyAdapter, "/")
    httpServer.start()
  }
  logger.info("Loki has started me={} i={} n={}", me, i_, n_)
}
