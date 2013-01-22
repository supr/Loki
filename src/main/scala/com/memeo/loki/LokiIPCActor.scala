package com.memeo.loki

import akka.actor.{ActorRef, Actor}
import akka.zeromq.{Frame, ZMQMessage}
import net.liftweb.json.JsonParser
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.Printer.compact

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/17/13
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
class LokiIPCActor(val service:ActorRef) extends Actor
{
  def receive = {
    case m:ZMQMessage => {
      JsonParser.parse(m.firstFrameAsString) match {
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
