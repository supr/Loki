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
import org.glassfish.grizzly.http.server.HttpHandler
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

class GrizzlyAdapter(val serviceActor:ActorRef)(implicit val context:ExecutionContext, implicit val system:ActorSystem) extends HttpHandler
{
  val utf8 = Charset.forName("UTF-8")
  val logger = Logging(system, classOf[GrizzlyAdapter])
  implicit val timeout = Timeout(30 seconds)

  override def service(request: org.glassfish.grizzly.http.server.Request,
                       response: org.glassfish.grizzly.http.server.Response):Unit = {
    logger.info("servicing {} {}", request.getMethod, request.getRequestURI)
    response.suspend()
    for {
      r <- serviceActor ? Request(request)
    } yield {
      r match {
        case resp:Response => {
          logger.info("service produced response {}", resp)
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
        }
        case _ => {
          response.setStatus(HttpStatus.NOT_FOUND_404)
        }
      }
      response.resume()
    }
  }
}
