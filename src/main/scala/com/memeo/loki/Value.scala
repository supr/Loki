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

import java.security.MessageDigest
import scala.{Boolean, Array}
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import java.io.{OutputStream, DataOutputStream}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JBool
import net.liftweb.json.JsonAST.JString
import akka.event.Logging
import akka.actor.ActorSystem

sealed abstract class DocumentMember
case object NullMember extends DocumentMember
case class BoolMember(value:Boolean) extends DocumentMember
case class IntMember(value:BigInt) extends DocumentMember
case class DoubleMember(value:Double) extends DocumentMember
case class StringMember(value:String) extends DocumentMember
case class ArrayMember(value:Array[DocumentMember]) extends DocumentMember
{
  override def toString:String = "ArrayMember(" + value.toList.toString + ")"
}
case class ObjectMember(value:Map[String, DocumentMember]) extends DocumentMember

class Document(val value:Map[String, DocumentMember])
class Revision(val seq:BigInt, val hash:String)

object Value
{
  def apply(id:Key, seq:BigInt, v:JValue, revs:List[Revision])(implicit system:ActorSystem):Value = {
    v match {
      case o:JObject => new Value(id, seq, false,
        new Document(JValueConversion.unpackValue(o).asInstanceOf[ObjectMember].value.filter(e => !e._1.startsWith("_"))),
        revs)
      case x => throw new IllegalArgumentException("invalid object: " + x)
    }
  }
}

class Value(val id:Key, val seq:BigInt,
            val deleted:Boolean,
            val value:Document,
            val revisions:List[Revision])
{
  def toValue():JValue = {
    deleted match {
      case true => ("_deleted" -> JBool(true)) ~ ("_id" -> JValueConversion.packKey(id)) ~ ("_rev" -> JString(genRev())) ~ Nil
      case false => JValueConversion.pack(ObjectMember(value.value)) merge JObject(List(JField("_id", JValueConversion.packKey(id)), JField("_rev", JString(genRev()))))
    }
  }

  def revHash():Array[Byte] = {
    Profiling.begin("compute_rev_hash")
    try {
      val md = MessageDigest.getInstance("SHA-1")
      val dos = new DataOutputStream(new OutputStream {
        def write(b: Int) = md.update(b.toByte)
        override def write(b:Array[Byte], offset:Int, length:Int) = md.update(b, offset, length)
      })
      val kser = new KeySerializer()
      kser.serialize(dos, 0, 1, Array(id))
      dos.writeBytes(seq.toString())
      val ser = new ValueSerializer()
      ser.serialize(dos, this)
      md.digest()
    } finally {
      Profiling.end("compute_rev_hash")
    }
  }

  def revStr():String = {
    revHash().foldLeft(new StringBuilder)((b, e) => b.append("%02x".format(e & 0xFF))).toString
  }

  def genRev():String = {
    "%d-%s".format(seq, revStr())
  }
}
