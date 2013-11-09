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

import org.mapdb.{BTreeKeySerializer, Serializer}
import java.io._
import java.nio.charset.Charset
import scala.Serializable
import collection.mutable.ArrayBuffer
import java.math.BigInteger
import collection.mutable
import akka.event.Logging
import akka.actor.ActorSystem

object KeySerializer
{
  val logger = Logging(ActorSystem("loki"), classOf[KeySerializer])
  val utf8 = Charset.forName("UTF-8")

  val TypeNull   = 'n'.toByte
  val TypeFalse  = 'f'.toByte
  val TypeTrue   = 't'.toByte
  val TypeInt    = 'i'.toByte
  val TypeBigInt = 'I'.toByte
  val TypeDouble = 'd'.toByte
  val TypeString = 's'.toByte
  val TypeArray  = '['.toByte
  val TypeObject = '{'.toByte
  val TypeKeys   = 'k'.toByte
  val TypeEnd    = 0.toByte
  val TypeSerializable = 'z'.toByte
  val TypeNullRef = '0'.toByte
}

class KeySerializer extends BTreeKeySerializer[Key] with Serializable
{
  def serializeKey(out:DataOutput, key:Key):Unit = key match {
    case NullKey => out.write(KeySerializer.TypeNull)
    case BoolKey(false) => out.write(KeySerializer.TypeFalse)
    case BoolKey(true) => out.write(KeySerializer.TypeTrue)
    case i:IntKey => {
      if (i.value.isValidInt)
      {
        out.write(KeySerializer.TypeInt)
        out.writeInt(i.value.toInt)
      }
      else
      {
        val value = i.value.underlying().toByteArray
        out.write(KeySerializer.TypeBigInt)
        out.writeInt(value.length)
        out.write(value)
      }
    }
    case d:DoubleKey => {
      out.write(KeySerializer.TypeDouble)
      out.writeDouble(d.value)
    }
    case s:StringKey => {
      val value = s.value.getBytes(KeySerializer.utf8)
      out.write(KeySerializer.TypeString)
      out.writeInt(value.length)
      out.write(value)
    }
    case a:ArrayKey => {
      out.write(KeySerializer.TypeArray)
      a.value.foreach(e => serializeKey(out, e))
      out.write(KeySerializer.TypeEnd)
    }
    case o:ObjectKey => {
      out.write(KeySerializer.TypeObject)
      o.value.foreach(e => serializeKey(out, new ArrayKey(Array(new StringKey(e._1), e._2))))
      out.write(KeySerializer.TypeEnd)
    }
  }

  def serialize(out: DataOutput, start: Int, end: Int, keys: Array[AnyRef]):Unit = {
    Profiling.begin("serialize_key")
    keys.slice(start, end).foreach(k => k match {
      case k:Key => serializeKey(out, k)
      case s:Serializable => {
        val b = new ByteArrayOutputStream()
        val o = new ObjectOutputStream(b)
        o.writeObject(s)
        o.close()
        val ser = b.toByteArray
        out.write(KeySerializer.TypeSerializable)
        out.writeInt(ser.length)
        out.write(ser)
      }
      case null => {
        out.write(KeySerializer.TypeNullRef)
      }
    })
    Profiling.end("serialize_key")
  }

  def deserializeKey(in: DataInput): Key = {
    deserializeKey(in, in.readByte())
  }

  def deserializeArray(in: DataInput, buf:ArrayBuffer[Key]):Unit = {
    in.readByte() match {
      case KeySerializer.TypeEnd => Unit
      case x => {
        buf += deserializeKey(in, x)
        deserializeArray(in, buf)
      }
    }
  }

  def deserializeObject(in: DataInput, map:mutable.LinkedHashMap[String, Key]):Unit = {
    in.readByte() match {
      case KeySerializer.TypeEnd => Unit
      case KeySerializer.TypeArray => {
        if (in.readByte() != KeySerializer.TypeString)
          throw new IOException("invalid type tag (expecting string for object key)")
        val key = deserializeKey(in, KeySerializer.TypeString).asInstanceOf[StringKey].value
        val value = deserializeKey(in)
        if (in.readByte() != KeySerializer.TypeEnd)
          throw new IOException("invalid type tag (expecting end of sequence)")
        map += (key -> value)
        deserializeObject(in, map)
      }
      case _ => throw new IOException("invalid type tag")
    }
  }

  def deserializeKey(in: DataInput, t:Byte):Key = {
    t match {
      case KeySerializer.TypeNull => NullKey
      case KeySerializer.TypeFalse => BoolKey(value = false)
      case KeySerializer.TypeTrue => BoolKey(value = true)
      case KeySerializer.TypeDouble => {
        DoubleKey(in.readDouble())
      }
      case KeySerializer.TypeInt => IntKey(BigInt(in.readInt()))
      case KeySerializer.TypeBigInt => {
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        IntKey(BigInt(new BigInteger(v)))
      }
      case KeySerializer.TypeString => {
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        StringKey(new String(v, KeySerializer.utf8))
      }
      case KeySerializer.TypeArray => {
        val buf = new ArrayBuffer[Key]()
        deserializeArray(in, buf)
        ArrayKey(buf.toArray)
      }
      case KeySerializer.TypeObject => {
        val map = new mutable.LinkedHashMap[String, Key]()
        deserializeObject(in, map)
        ObjectKey(map.toMap)
      }
    }
  }

  def deserialize(in: DataInput, start: Int, end: Int, size: Int): Array[AnyRef] = {
    Profiling.begin("deserialize_key")
    KeySerializer.logger.debug("deserialize {} {} {}", start, end, size)
    val buf = new ArrayBuffer[AnyRef](size)
    Range(0, start, 1).foreach(i => {
      KeySerializer.logger.debug("prepend null {}", i)
      buf += null
    })
    Range(start, end, 1).foreach(i => {
      KeySerializer.logger.debug("deserialize {}", i)
      in.readByte() match {
        case KeySerializer.TypeSerializable => {
          val len = in.readInt()
          val v = new Array[Byte](len)
          in.readFully(v)
          val i = new ObjectInputStream(new ByteArrayInputStream(v))
          buf += i.readObject()
        }
        case t => buf += deserializeKey(in, t)
        case x => throw new IOException("invalid type code " + x)
      }
    })
    Range(end, size, 1).foreach(i => {
      KeySerializer.logger.debug("append null {}", i)
      buf += null
    })
    val ret = buf.toArray
    Profiling.end("deserialize_key")
    ret
  }
}
