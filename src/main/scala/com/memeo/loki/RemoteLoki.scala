package com.memeo.loki

import akka.actor.{ActorSystem, Actor}
import akka.pattern.{ask, pipe}
import akka.zeromq.{Frame, ZMQMessage, Connect, ZeroMQExtension}

import net.liftweb.json.{compact, render}
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonParser.parse
import akka.zeromq.Connect
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JArray
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import akka.util.{ByteString, Timeout}
import concurrent.duration._
import akka.event.Logging
import java.nio.charset.Charset
import java.io.{InputStreamReader, ByteArrayInputStream}
import concurrent.Await
import akka.actor.Status.Failure

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
class RemoteLoki(val peer:Peer)(implicit val system:ActorSystem) extends Actor
{
  val logger = Logging(system, classOf[RemoteLoki])
  val socket = ZeroMQExtension(system).newReqSocket(Array(Connect(peer.ipcAddr)))
  implicit val timeout = Timeout(30 seconds)
  import context.dispatcher
  def receive = {
    case r:Request => {
      logger.info("sending message {} to {}", r, peer.ipcAddr)
      val msg = ZMQMessage(List(Frame(Array[Byte]()), Frame(compact(render(r.toValue)))))
      val response = Await.result(socket ? msg map {
        x => x match {
          case res:ZMQMessage => parse(new InputStreamReader(new ByteArrayInputStream(res.frames(1).payload.toArray))) match {
            case JArray(List(r:JInt, v:JValue, h:JObject)) => {
              logger.info("got response {} {} {}", r, v, h)
              Response(HttpResponseStatus.valueOf(r.values.intValue()), v, h)
            }
          }
          case x => {
            logger.warning("unexpected reply {}", x)
            Failure(new Exception("unexpected reply"))
          }
        }
      }, 30 seconds)
      logger.info("sending peer response back {} {}", response, sender)
      sender ! response
    }
  }
}
