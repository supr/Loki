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
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.collection.mutable.ListBuffer
import com.memeo.loki.api.{AST, RowEmitter}
import net.liftweb.json.JsonAST.JValue
import scala.collection.convert.WrapAsJava
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicReference

class MapReduceActor extends Actor
{
  def timeoutFuture[A](timeout:FiniteDuration)(block: => A): Future[A] = {
    val prom = promise[A]
    context.system.scheduler.scheduleOnce(timeout) {
      prom tryFailure new TimeoutException
    }
    future {
      prom success block
    }
    prom.future
  }

  def receive = {
    case mr:MapRequest => {
      timeoutFuture(1 minutes) {
        val mapper = MapReduce(mr.mapfun)
        val rows:ListBuffer[(JValue, JValue)] = new ListBuffer[(JValue, JValue)]
        mr.docs.foreach(doc => mapper.map(new api.AST.ObjectValue(doc), new RowEmitter {
          def emit(key: AST.Value[_], value: AST.Value[_]) = rows += (key.toJValue -> value.toJValue)
        }))
        rows.toList
      } onComplete {
        case s:Success[List[(JValue, JValue)]] => sender ! new MapResult(mr.uuid, s)
        case f:Failure => sender ! new MapResult(mr.uuid, f)
      }
    }

    case rr:ReduceRequest => {
      timeoutFuture(1 minutes) {
        val reducer = MapReduce(rr.reducefun)
        val reduced:api.AST.Value[_] = reducer.reduce(WrapAsJava.seqAsJavaList(rr.keys.map(l => l.map {k => api.AST.of(k)}).getOrElse(List())),
                                                      WrapAsJava.seqAsJavaList(rr.values.map(v => api.AST.of(v))), rr.rereduce)
        reduced.toJValue
      } onComplete {
        case s:Success[JValue] => sender ! new ReduceResult(rr.uuid, s)
        case f:Failure => sender ! new ReduceResult(rr.uuid, f)
      }
    }
  }
}
