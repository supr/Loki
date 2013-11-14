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
import com.sleepycat.je.DatabaseEntry
import com.sleepycat.bind.{EntryBinding, EntityBinding}

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
  def serialize(out: DataOutput, value: Value) = {
    Profiling.begin("serialize_value")
    try {
      out.write(ValueSerializer.BeginValue)
      out.write(ValueSerializer.ValueId)
      val kser = new KeySerializer()
      kser.serializeKey(out, value.id)
      out.write(ValueSerializer.SequenceId)
      val v = value.seq.underlying().toByteArray()
      out.writeInt(v.length)
      out.write(v)
      if (value.deleted) {
        out.writeBoolean(true)
        out.write(ValueSerializer.TypeArray)
        value.revisions.foreach(rev => serializeMember(out, ArrayMember(Array(IntMember(rev.seq), StringMember(rev.hash)))))
        out.write(ValueSerializer.TypeEnd)
      }
      else
      {
        out.writeBoolean(false)
        serializeMember(out, ObjectMember(value.value.value))
        out.write(ValueSerializer.TypeArray)
        value.revisions.foreach(rev => serializeMember(out, ArrayMember(Array(IntMember(rev.seq), StringMember(rev.hash)))))
        out.write(ValueSerializer.TypeEnd)
      }
      out.write(ValueSerializer.EndValue)
    } finally {
      Profiling.end("serialize_value")
    }
  }

  def serializeMember(out: DataOutput, value: DocumentMember):Unit = {
    ValueSerializer.logger.debug("serializeMember {}", value)
    value match {
      case NullMember => out.write(ValueSerializer.TypeNull)
      case BoolMember(false) => out.write(ValueSerializer.TypeFalse)
      case BoolMember(true) => out.write(ValueSerializer.TypeTrue)
      case i:IntMember => {
        if (i.value.isValidInt)
        {
          out.write(ValueSerializer.TypeInt)
          out.writeInt(i.value.intValue())
        }
        else
        {
          val v = i.value.underlying().toByteArray()
          out.write(ValueSerializer.TypeBigInt)
          out.writeInt(v.length)
          out.write(v)
        }
      }
      case d:DoubleMember => {
        out.write(ValueSerializer.TypeDouble)
        out.writeDouble(d.value)
      }
      case s:StringMember => {
        val v = s.value.getBytes(ValueSerializer.utf8)
        out.write(ValueSerializer.TypeString)
        out.writeInt(v.length)
        out.write(v)
      }
      case a:ArrayMember => {
        out.write(ValueSerializer.TypeArray)
        a.value.foreach(e => serializeMember(out, e))
        out.write(ValueSerializer.TypeEnd)
      }
      case o:ObjectMember => {
        out.write(ValueSerializer.TypeObject)
        o.value.foreach(e => serializeMember(out, new ArrayMember(Array(new StringMember(e._1), e._2))))
        out.write(ValueSerializer.TypeEnd)
      }
      case null => out.write(ValueSerializer.TypeNullRef)
    }
  }

  def deserializeMember(in:DataInput):DocumentMember = {
    deserializeMember(in, in.readByte())
  }

  def deserializeMember(in:DataInput, t:Byte):DocumentMember = {
    t match {
      case ValueSerializer.TypeNull => NullMember
      case ValueSerializer.TypeFalse => BoolMember(false)
      case ValueSerializer.TypeTrue => BoolMember(true)
      case ValueSerializer.TypeInt => new IntMember(BigInt(in.readInt()))
      case ValueSerializer.TypeBigInt => {
        val v = new Array[Byte](in.readInt())
        in.readFully(v)
        new IntMember(BigInt(new BigInteger(v)))
      }
      case ValueSerializer.TypeDouble => new DoubleMember(in.readDouble())
      case ValueSerializer.TypeString => {
        val v = new Array[Byte](in.readInt())
        in.readFully(v)
        new StringMember(new String(v, ValueSerializer.utf8))
      }
      case ValueSerializer.TypeArray => {
        val buf = new ArrayBuffer[DocumentMember]()
        deserializeArray(in, buf)
        new ArrayMember(buf.toArray)
      }
      case ValueSerializer.TypeObject => {
        val map = new mutable.LinkedHashMap[String, DocumentMember]()
        deserializeObject(in, map)
        new ObjectMember(map.toMap)
      }
      case ValueSerializer.TypeNullRef => null
    }
  }

  def deserializeArray(in:DataInput, buf:ArrayBuffer[DocumentMember]):Unit = {
    in.readByte() match {
      case ValueSerializer.TypeEnd => Unit
      case x => {
        buf += deserializeMember(in, x)
        deserializeArray(in, buf)
      }
    }
  }

  def deserializeObject(in:DataInput, map:mutable.LinkedHashMap[String, DocumentMember]):Unit = {
    in.readByte() match {
      case ValueSerializer.TypeArray => {
        val buf = new ArrayBuffer[DocumentMember](2)
        deserializeArray(in, buf)
        val kv = buf.toArray
        if (kv.length != 2 || !kv(0).isInstanceOf[StringMember])
          throw new IOException("invalid key value pair " + kv)
        map += (kv(0).asInstanceOf[StringMember].value -> kv(1))
        deserializeObject(in, map)
      }
      case ValueSerializer.TypeEnd => Unit
      case _ => throw new IOException("malformed serialized object (excepting key/value or end)")
    }
  }

  def deserializeDoc(in: DataInput):Document = {
    if (in.readByte() != ValueSerializer.TypeObject)
      throw new IOException("malformed serialized value (expecting begin root object)")
    val m = new mutable.LinkedHashMap[String, DocumentMember]()
    deserializeObject(in, m)
    new Document(m.toMap)
  }

  def deserializeRevisions(in:DataInput, buf:ArrayBuffer[Revision]):Unit = {
    in.readByte() match {
      case ValueSerializer.TypeEnd => Unit
      case ValueSerializer.TypeArray => {
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
    Profiling.begin("deserialize_value")
    val b = new Array[Byte](available)
    din.readFully(b)
    val in = new DataInputStream(new ByteArrayInputStream(b))
    try {
      in.readByte() match {
        case ValueSerializer.EndValue => null
        case ValueSerializer.BeginValue => {
          if (in.readByte() != ValueSerializer.ValueId)
            throw new IOException("malformed serialized value (no begin ID)")
          val kser = new KeySerializer()
          val id = kser.deserializeKey(in)
          if (in.readByte() != ValueSerializer.SequenceId)
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
            if (in.readByte() != ValueSerializer.TypeArray)
              throw new IOException("malformed revisions list")
            deserializeRevisions(in, revs)
            new Value(id, seq, true, doc, revs.toList)
          }
          else
          {
            val doc = deserializeDoc(in)
            val revs = new ArrayBuffer[Revision]()
            if (in.readByte() != ValueSerializer.TypeArray)
              throw new IOException("malformed revisions list")
            deserializeRevisions(in, revs)
            new Value(id, seq, false, doc, revs.toList)
          }
        }
        case x => throw new IOException("malformed serialized value: " + x)
      }
    } finally {
      Profiling.end("deserialize_value")
    }
  }

  def toBinary(o: Value): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    serialize(new DataOutputStream(out), o)
    out.toByteArray
  }

  def fromBinary(bytes: Array[Byte]): Value = {
    deserialize(new DataInputStream(new ByteArrayInputStream(bytes)), bytes.length)
  }

  def fromBinary(entry: DatabaseEntry): Value = {
    (entry.getOffset, entry.getSize) match {
      case (o, l) if o == 0 && l == entry.getData.length => fromBinary(entry.getData)
      case (o, l) => fromBinary(entry.getData.slice(o, l + o))
    }
  }
}

class ValueBinding extends EntryBinding[Value]
{
  val serial = new ValueSerializer
  def objectToEntry(value: Value, entry: DatabaseEntry) = entry.setData(serial.toBinary(value))
  def entryToObject(entry: DatabaseEntry): Value = serial.fromBinary(entry)
}
