package com.memeo.loki

import akka.actor.{ActorRef, Actor}
import akka.pattern.{ask, pipe}
import akka.zeromq.{Connecting, Closed, Frame, ZMQMessage}
import net.liftweb.json.JsonParser
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.Printer.compact
import akka.event.Logging
import java.io.{ByteArrayInputStream, InputStreamReader}
import akka.util.Timeout
import concurrent.duration._
import concurrent.Await
import akka.actor.Status.Failure

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
  implicit val timeout:Timeout = Timeout(30 seconds)

  def receive = {
    case m:ZMQMessage => {
      import context.dispatcher
      logger.info("received message {} sender={}", m, sender)
      JsonParser.parse(new InputStreamReader(new ByteArrayInputStream(m.frames(1).payload.toArray))) match {
        case JArray(List(method:JString, uri:JString, value:JValue, headers:JObject, params:JObject)) =>
          val reply = Await.result(service ? Request(method.values, uri.values, value, headers, params) map {
            x => x match {
              case r:Response => {
                logger.info("got response {}, forwarding back {}", r.toValue(), sender)
                ZMQMessage(List(Frame(Array[Byte]()), Frame(compact(render(r.toValue())))))
              }
              case x => {
                logger.warning("unexpected reply {}", x)
                Failure(new IllegalArgumentException("unexpected reply type"))
              }
            }
          }, 30 seconds)
          logger.info("sending reply {} {}", reply, sender)
          sender ! reply
      }
    }

    case Connecting => {
      logger.info("req connecting")
    }

    case Closed => {
      logger.info("req closed")
    }
  }
}
