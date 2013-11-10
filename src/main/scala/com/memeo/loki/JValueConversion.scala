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
import scala.collection.mutable
import collection.mutable.ArrayBuffer
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
  def unpackKey(key:JValue):Key = {
    key match {
      case JNothing|JNull => NullKey
      case JBool(false) => BoolKey(false)
      case JBool(true) => BoolKey(true)
      case i:JInt => new IntKey(i.values)
      case f:JDouble => new DoubleKey(f.values)
      case s:JString => new StringKey(s.values)
      case a:JArray => {
        new ArrayKey(a.children.foldLeft(new ArrayBuffer[Key]())((a, e) => {
          a += unpackKey(e)
          a
        }).toArray)
      }
      case o:JObject => {
        new ObjectKey(o.obj.foldLeft(new mutable.LinkedHashMap[String, Key]())((m, e) => {
          m += (e.name -> unpackKey(e.value))
        }).toMap)
      }
    }
  }

  def unpackValue(value:JValue):DocumentMember = {
    value match {
      case JNothing|JNull => NullMember
      case b:JBool => new BoolMember(b.value)
      case i:JInt => new IntMember(i.values)
      case f:JDouble => new DoubleMember(f.values)
      case s:JString => new StringMember(s.values)
      case a:JArray => {
        new ArrayMember(a.children.foldLeft(new ArrayBuffer[DocumentMember]())((a, e) => {
          a += unpackValue(e)
          a
        }).toArray)
      }
      case o:JObject => {
        new ObjectMember(o.obj.foldLeft(new mutable.LinkedHashMap[String, DocumentMember]())((m, e) => {
          m += (e.name -> unpackValue(e.value))
          m
        }).toMap)
      }
    }
  }

  def unpackDoc(value:JObject):Document = {
    new Document(value.obj.map {
      f => (f.name -> unpackValue(f.value))
    }.toMap)
  }

  def pack(value:DocumentMember):JValue = value match {
    case NullMember => JNull
    case BoolMember(false) => JBool(false)
    case BoolMember(true) => JBool(true)
    case f:DoubleMember => JDouble(f.value)
    case i:IntMember => JInt(i.value)
    case s:StringMember => JString(s.value)
    case l:ArrayMember => JArray(l.value.map(pack).toList)
    case m:ObjectMember => JObject(m.value.map(e => JField(e._1, pack(e._2))).toList)
  }

  def pack(doc:Document):JValue = {
    JObject(doc.value.map { e => JField(e._1, pack(e._2))}.toList)
  }

  def packKey(key:Key):JValue = key match {
    case NullKey => JNull
    case BoolKey(false) => JBool(false)
    case BoolKey(true) => JBool(true)
    case f:DoubleKey => JDouble(f.value)
    case i:IntKey => JInt(i.value)
    case s:StringKey => JString(s.value)
    case i:ArrayKey => JArray(i.value.map(packKey).toList)
    case o:ObjectKey => JObject(o.value.map(e => JField(e._1, packKey(e._2))).toList)
  }

  def packRevs(list:List[Revision]):JArray = {
    JArray(list.map {rev => JArray(List(JInt(rev.seq), JString(rev.hash)))})
  }

  def unpackRevs(list:JArray):List[Revision] = {
    list.arr.map {
      case JArray(List(seq: JInt, hash: JString)) => new Revision(seq.values, hash.values)
      case _ => throw new IllegalArgumentException
    }
  }
}
