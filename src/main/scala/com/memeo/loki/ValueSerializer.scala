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

import org.mapdb.Serializer
import java.io._
import java.nio.charset.Charset
import java.math.BigInteger
import collection.mutable
import collection.mutable.ArrayBuffer
import akka.event.Logging
import akka.actor.ActorSystem
import com.memeo.loki.BoolMember
import com.memeo.loki.StringMember
import com.memeo.loki.ObjectMember
import com.memeo.loki.DoubleMember
import com.memeo.loki.ArrayMember
import scala.Serializable
import com.memeo.loki.IntMember

object ValueSerializer
{
  val logger = Logging(ActorSystem("loki"), classOf[ValueSerializer])
  val utf8 = Charset.forName("UTF-8")

  val BeginValue   = 'v'.toByte
  val ValueId      = 'i'.toByte
  val SequenceId   = 's'.toByte
  val EndValue     =  0 .toByte

  val TypeNull   = 'n'.toByte
  val TypeFalse  = 'f'.toByte
  val TypeTrue   = 't'.toByte
  val TypeInt    = 'i'.toByte
  val TypeBigInt = 'I'.toByte
  val TypeDouble = 'd'.toByte
  val TypeString = 's'.toByte
  val TypeArray  = '['.toByte
  val TypeObject = '{'.toByte
  val TypeEnd    = 0.toByte
  val TypeNullRef = '0'.toByte
}

class ValueSerializer extends Serializer[Value] with Serializable
{
  import ValueSerializer._

  def serialize(out: DataOutput, value: Value) = {
    out.write(BeginValue)
    out.write(ValueId)
    val kser = new KeySerializer()
    kser.serializeKey(out, value.id)
    out.write(SequenceId)
    val v = value.seq.underlying().toByteArray()
    out.writeInt(v.length)
    out.write(v)
    if (value.deleted) {
      out.writeBoolean(true)
      out.write(TypeArray)
      value.revisions.foreach(rev => serializeMember(out, ArrayMember(Array(IntMember(rev.seq), StringMember(rev.hash)))))
      out.write(TypeEnd)
    }
    else
    {
      out.writeBoolean(false)
      serializeMember(out, ObjectMember(value.value.value))
      out.write(TypeArray)
      value.revisions.foreach(rev => serializeMember(out, ArrayMember(Array(IntMember(rev.seq), StringMember(rev.hash)))))
      out.write(TypeEnd)
    }
    out.write(EndValue)
  }

  def serializeMember(out: DataOutput, value: DocumentMember):Unit = {
    logger.info("serializeMember {}", value)
    value match {
      case NullMember => out.write(TypeNull)
      case BoolMember(false) => out.write(TypeFalse)
      case BoolMember(true) => out.write(TypeTrue)
      case i:IntMember => {
        if (i.value.isValidInt)
        {
          out.write(TypeInt)
          out.writeInt(i.value.intValue())
        }
        else
        {
          val v = i.value.underlying().toByteArray()
          out.write(TypeBigInt)
          out.writeInt(v.length)
          out.write(v)
        }
      }
      case d:DoubleMember => {
        out.write(TypeDouble)
        out.writeDouble(d.value)
      }
      case s:StringMember => {
        val v = s.value.getBytes(utf8)
        out.write(TypeString)
        out.writeInt(v.length)
        out.write(v)
      }
      case a:ArrayMember => {
        out.write(TypeArray)
        a.value.foreach(e => serializeMember(out, e))
        out.write(TypeEnd)
      }
      case o:ObjectMember => {
        out.write(TypeObject)
        o.value.foreach(e => serializeMember(out, new ArrayMember(Array(new StringMember(e._1), e._2))))
        out.write(TypeEnd)
      }
      case null => out.write(TypeNullRef)
    }
  }

  def deserializeMember(in:DataInput):DocumentMember = {
    deserializeMember(in, in.readByte())
  }

  def deserializeMember(in:DataInput, t:Byte):DocumentMember = {
    t match {
      case TypeNull => NullMember
      case TypeFalse => BoolMember(false)
      case TypeTrue => BoolMember(true)
      case TypeInt => new IntMember(BigInt(in.readInt()))
      case TypeBigInt => {
        val v = new Array[Byte](in.readInt())
        in.readFully(v)
        new IntMember(BigInt(new BigInteger(v)))
      }
      case TypeDouble => new DoubleMember(in.readDouble())
      case TypeString => {
        val v = new Array[Byte](in.readInt())
        in.readFully(v)
        new StringMember(new String(v, utf8))
      }
      case TypeArray => {
        val buf = new ArrayBuffer[DocumentMember]()
        deserializeArray(in, buf)
        new ArrayMember(buf.toArray)
      }
      case TypeObject => {
        val map = new mutable.LinkedHashMap[String, DocumentMember]()
        deserializeObject(in, map)
        new ObjectMember(map.toMap)
      }
      case TypeNullRef => null
    }
  }

  def deserializeArray(in:DataInput, buf:ArrayBuffer[DocumentMember]):Unit = {
    in.readByte() match {
      case TypeEnd => Unit
      case x => {
        buf += deserializeMember(in, x)
        deserializeArray(in, buf)
      }
    }
  }

  def deserializeObject(in:DataInput, map:mutable.LinkedHashMap[String, DocumentMember]):Unit = {
    in.readByte() match {
      case TypeArray => {
        val buf = new ArrayBuffer[DocumentMember](2)
        deserializeArray(in, buf)
        val kv = buf.toArray
        if (kv.length != 2 || !kv(0).isInstanceOf[StringMember])
          throw new IOException("invalid key value pair " + kv)
        map += (kv(0).asInstanceOf[StringMember].value -> kv(1))
        deserializeObject(in, map)
      }
      case TypeEnd => Unit
      case _ => throw new IOException("malformed serialized object (excepting key/value or end)")
    }
  }

  def deserializeDoc(in: DataInput):Document = {
    if (in.readByte() != TypeObject)
      throw new IOException("malformed serialized value (expecting begin root object)")
    val m = new mutable.LinkedHashMap[String, DocumentMember]()
    deserializeObject(in, m)
    new Document(m.toMap)
  }

  def deserializeRevisions(in:DataInput, buf:ArrayBuffer[Revision]):Unit = {
    in.readByte() match {
      case TypeEnd => Unit
      case TypeArray => {
        val a = new ArrayBuffer[DocumentMember]()
        deserializeArray(in, a)
        if (a.size != 2 || !a(0).isInstanceOf[IntMember] || !a(1).isInstanceOf[StringMember])
          throw new IOException("revisions must be two-element arrays")
        buf += new Revision(a(0).asInstanceOf[IntMember].value, a(1).asInstanceOf[StringMember].value)
        deserializeRevisions(in, buf)
      }
    }
  }

  def deserialize(din: DataInput, available: Int): Value = {
    val b = new Array[Byte](available)
    din.readFully(b)
    val in = new DataInputStream(new ByteArrayInputStream(b))
    in.readByte() match {
      case EndValue => null
      case BeginValue => {
        if (in.readByte() != ValueId)
          throw new IOException("malformed serialized value (no begin ID)")
        val kser = new KeySerializer()
        val id = kser.deserializeKey(in)
        if (in.readByte() != SequenceId)
          throw new IOException("malformed serialized value (no begin seq)")
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        val seq = BigInt(new BigInteger(v))
        val deleted = in.readBoolean()
        if (deleted)
        {
          val doc = new Document(Map())
          val revs = new ArrayBuffer[Revision]()
          if (in.readByte() != TypeArray)
            throw new IOException("malformed revisions list")
          deserializeRevisions(in, revs)
          new Value(id, seq, true, doc, revs.toList)
        }
        else
        {
          val doc = deserializeDoc(in)
          val revs = new ArrayBuffer[Revision]()
          if (in.readByte() != TypeArray)
            throw new IOException("malformed revisions list")
          deserializeRevisions(in, revs)
          new Value(id, seq, false, doc, revs.toList)
        }
      }
      case x => throw new IOException("malformed serialized value: " + x)
    }
  }
}
