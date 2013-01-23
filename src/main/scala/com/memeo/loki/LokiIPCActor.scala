package com.memeo.loki

import akka.actor.{ActorRef, Actor}
import akka.zeromq.{Frame, ZMQMessage}
import net.liftweb.json.JsonParser
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.Printer.compact
import akka.event.Logging
import java.io.{ByteArrayInputStream, InputStreamReader}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/17/13
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
class LokiIPCActor(val service:ActorRef) extends Actor
{
  val logger = Logging(context.system, classOf[LokiIPCActor])

  def receive = {
    case m:ZMQMessage => {
      logger.info("received message {}", m)
      JsonParser.parse(new InputStreamReader(new ByteArrayInputStream(m.frames(1).payload.toArray))) match {
        case JArray(List(method:JString, uri:JString, params:JObject, headers:JObject, value:JValue)) =>
          service ! (sender, Request(method.values, uri.values, value, headers, params))
      }
    }

    case (upstream:ActorRef, resp:Response) => {
      val reply = JArray(List(JInt(resp.status.getCode),
        resp.headers, resp.value))
      upstream ! new ZMQMessage(new Frame(compact(render(reply))))
    }
  }
}
