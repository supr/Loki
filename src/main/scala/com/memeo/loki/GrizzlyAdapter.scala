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

import akka.actor.Actor
import scala.concurrent.{future, promise}
import scala.concurrent.duration._
import org.glassfish.grizzly.http.server.{HttpServer, HttpHandler}
import akka.actor.{ActorSystem, ActorRef}
import akka.pattern.ask
import net.liftweb.json.JsonAST._
import net.liftweb.json.Printer.compact
import akka.util.Timeout
import scala.concurrent.ExecutionContext
import akka.event.Logging
import org.glassfish.grizzly.http.util.HttpStatus
import java.nio.charset.Charset
import org.glassfish.grizzly.http.io.BinaryNIOOutputSink
import org.glassfish.grizzly.memory.ByteBufferWrapper
import java.nio.ByteBuffer
import org.glassfish.grizzly.{ReadHandler, http}
import java.io.{ByteArrayInputStream, InputStreamReader, ByteArrayOutputStream}
import net.liftweb.json.JsonParser
import scala.util.{Try, Success, Failure}

class GrizzlyAdapter(val serviceActor:ActorRef) extends HttpHandler with Actor
{
  import ExecutionContext.Implicits.global

  val utf8 = Charset.forName("UTF-8")
  val logger = Logging(context.system, classOf[GrizzlyAdapter])
  implicit val timeout = Timeout(30 seconds)

  type HttpRequest = org.glassfish.grizzly.http.server.Request
  type HttpResponse = org.glassfish.grizzly.http.server.Response

  def receive = {
    case server:HttpServer => {
      server.getServerConfiguration.addHttpHandler(this, "/")
      sender ! "OK"
    }

    case resp:Response => {
      logger.debug("service produced response {}", resp)
      resp.baton match {
        case s:Some[AnyRef] if s.get.isInstanceOf[HttpResponse] => {
          val response = s.get.asInstanceOf[HttpResponse]
          response.setStatus(resp.status.id)
          resp.headers.values.foreach {
            e => response.setHeader(e._1, e._2.toString)
          }
          resp.value match {
            case JNothing => Unit
            case v:JValue => {
              val json = compact(render(v)).getBytes(utf8)
              response.setContentType("application/json")
              response.setContentLength(json.length)
              response.getNIOOutputStream.asInstanceOf[BinaryNIOOutputSink].write(new ByteBufferWrapper(ByteBuffer.wrap(json)))
            }
          }
          response.resume()
        }
        case _ => {
          logger.warning("expecting response with HTTP response, got {}", resp.baton)
        }
      }
    }
  }

  override def service(request: HttpRequest, response: HttpResponse):Unit = {
    logger.info("servicing {} {}", request.getMethod, request.getRequestURI)
    response.suspend()
    val requestPromise = promise[Try[Request]]()
    request.getMethod match {
      case http.Method.PUT | http.Method.POST => {
        val buffer = request.getContentLength match {
          case i:Int if i <= 0 => new ByteArrayOutputStream()
          case i:Int => new ByteArrayOutputStream(i)
        }
        val input = request.getNIOInputStream
        input.notifyAvailable(new ReadHandler {
          def onError(t: Throwable) = {
            logger.warning("onError {}", t)
            requestPromise.success(Failure(t))
          }

          def onDataAvailable() = {
            logger.debug("onDataAvailable")
            val available = input.available()
            logger.debug("available={}", available)
            val buf = new Array[Byte](available)
            val read = input.read(buf)
            buffer.write(buf, 0, read)
            logger.debug("read {} bytes of body", read)
          }

          def onAllDataRead() = {
            logger.debug("onAllDataRead()")
            val available = input.available()
            logger.debug("available={}", available)
            if (available > 0) {
              val buf = new Array[Byte](available)
              input.read(buf)
              buffer.write(buf)
            }
            val req = Request(request, JsonParser.parse(new InputStreamReader(new ByteArrayInputStream(buffer.toByteArray))), Some(response))
            requestPromise.success(Success(req))
            logger.debug("completed request: {}", req)
          }
        })
      }
      case _ => requestPromise.success(Success(Request(request, JNothing, Some(response))))
    }
    for {
      result <- requestPromise.future
    } yield {
      result match {
        case req:Success[Request] => {
          logger.info("sending request {} to service")
          serviceActor ! req.get
        }
        case fail:Failure[Request] => {
          response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500)
          val result = JObject(List(JField("error", JBool(value = true)), JField("reason", JString(fail.exception.getMessage))))
          val json = compact(render(result)).getBytes(utf8)
          response.setContentType("application/json")
          response.setContentLength(json.length)
          response.getNIOOutputStream.asInstanceOf[BinaryNIOOutputSink].write(new ByteBufferWrapper(ByteBuffer.wrap(json)))
          response.resume()
        }
        case _ => {
          response.setStatus(HttpStatus.NOT_FOUND_404)
          response.resume()
        }
      }
    }
  }
}
