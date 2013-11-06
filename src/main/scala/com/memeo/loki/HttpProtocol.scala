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

import org.scalatra.{LifeCycle, ScalatraServlet}
import javax.servlet.ServletContext
import akka.actor.{Actor, ActorRef}
import net.liftweb.json.JsonAST._
import scala.collection.mutable.ListBuffer
import java.net.URLDecoder._
import scala.Some
import net.liftweb.json.JsonAST.JField
import net.liftweb.json.JsonAST.JObject
import scala.Some
import java.util

class LokiBootstrap(val serviceActor:ActorRef) extends LifeCycle
{
  override def init(context: ServletContext) {
    context.mount(new HttpProtocol(serviceActor), "/*")
  }
}

class HttpProtocol(val serviceActor:ActorRef) extends ScalatraServlet
{
  def parseQuery(query:String):JObject = {
    Option(query) match {
      case None => JObject(List())
      case s:Some[String] => {
        JObject(s.get.split('&').foldLeft[ListBuffer[JField]](new ListBuffer[JField]())((l, e) => {
          val p = e.split("=")
          l += JField(decode(p(0), "UTF-8"), JString(decode(p(1), "UTF-8")))
        }).result())
      }
    }
  }

  def getHeaders():JObject = {
    JObject(scala.collection.convert.Wrappers.JEnumerationWrapper(request.getHeaderNames).map(key => {
      key match {
        case name:String => (name,
          scala.collection.convert.Wrappers.JEnumerationWrapper(request.getHeaders(name)))
      }
    }).foldLeft[ListBuffer[JField]](new ListBuffer[JField]())((l, e) => {
      l ++= e._2.map(v => v match {
        case value:String => JField(e._1, JString(value))
      })
    }).result())
  }

  get("/*") {
    (serviceActor ! Request(Method.GET, request.getRequestURI, JNothing, getHeaders(), JObject(List()))) match {
      case response:Response => {
        halt(status = response.status.id, body = response.value, headers = response.headers)
      }
    }
  }
}
