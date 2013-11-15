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

import java.util.UUID
import scala.util.Try
import net.liftweb.json.JsonAST.{JValue, JObject}

object Language extends Enumeration
{
  type Language = Value
  val javascript, groovy, ruby, python, scala, java = Value
}

class MapFunction(val name:String, val code:String, val lang:Language.Language)
class ReduceFunction(val name:String, val code:String, val lang:Language.Language)

class MapRequest(val mapfun:MapFunction,
                 val docs:List[JObject],
                 val uuid:UUID = UUID.randomUUID())
class MapResult(val uuid:UUID,
                val values:Try[List[(JValue, JValue)]])
class ReduceRequest(val reducefun:ReduceFunction,
                    val keys:Option[List[JValue]],
                    val values:List[JValue],
                    val rereduce:Boolean,
                    val uuid:UUID = UUID.randomUUID())
class ReduceResult(val uuid:UUID,
                   val value:Try[JValue])

object MapReduce
{
  def apply(map:MapFunction):api.MapFunction = map.lang match {
    case Language.javascript => new JavascriptMapAdapter(map.code)
    case _ => throw new UnsupportedOperationException(map.lang + ": not yet implemented")
  }

  def apply(reduce:ReduceFunction):api.ReduceFunction = reduce.lang match {
    case Language.javascript => new JavascriptReduceAdapter(reduce.code)
    case _ => throw new UnsupportedOperationException(reduce.lang + ": not yet implemented")
  }
}