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

import org.mapdb.{Serializer, SerializerBase}
import net.liftweb.json.JsonAST._
import collection.convert.{WrapAsScala, WrapAsJava}
import java.math.BigInteger
import scala._
import net.liftweb.json.JsonAST.JDouble
import net.liftweb.json.JsonAST.JBool
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JArray
import java.io.{DataOutput, DataInput}
import java.util

object JValueConversion
{
  def unpack(value:JValue):Object = {
    value match {
      case JNothing => null
      case JNull => null
      case b:JBool => java.lang.Boolean.valueOf(b.value)
      case i:JInt => i.values.underlying()
      case f:JDouble => java.lang.Double.valueOf(f.values)
      case s:JString => s.values
      case a:JArray => {
        a.children.foldLeft(new util.ArrayList[Object]())((a, e) => {
          a.add(unpack(e))
          a
        })
      }
      case o:JObject => {
        o.obj.foldLeft(new util.LinkedHashMap[String, Object]())((m, e) => {
          m.put(e.name, unpack(e.value))
          m
        })
      }
    }
  }

  def pack(value:Any):JValue = {
    value match {
      case null => JNull
      case b:Boolean => JBool(b)
      case f:Double => JDouble(f)
      case i:BigInteger => JInt(BigInt(i))
      case l:Long => JInt(BigInt(l))
      case i:Integer => JInt(BigInt(i))
      case s:String => JString(s)
      case l:java.util.List[Any] => JArray(WrapAsScala.iterableAsScalaIterable(l).map(pack).toList)
      case m:java.util.Map[String, Any] => JObject(WrapAsScala.iterableAsScalaIterable(m.entrySet).map((e:java.util.Map.Entry[String, Any]) => JField(e.getKey, pack(e.getValue))).toList)
    }
  }
}
