package com.memeo.loki

import akka.actor.{ActorSystem, Actor}
import akka.pattern.{ask, pipe}
import akka.zeromq.{Frame, ZMQMessage, Connect, ZeroMQExtension}

import net.liftweb.json.{compact, parse, render}
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonAST.JObject
import akka.zeromq.Connect
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JArray
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import akka.util.{ByteString, Timeout}
import concurrent.duration._
import akka.event.Logging
import java.nio.charset.Charset

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
      val msg = ZMQMessage(List(Frame(peer.name), Frame(compact(render(r.toValue)))))
      socket ? msg map {
        x => x match {
          case res:ZMQMessage => parse(res.firstFrameAsString) match {
            case JArray(List(r:JInt, v:JValue, h:JObject)) => {
              logger.info("got response {} {} {}", r, v, h)
              Response(HttpResponseStatus.valueOf(r.values.intValue()), v, h)
            }
          }
        }
      } pipeTo sender
    }
  }
}
