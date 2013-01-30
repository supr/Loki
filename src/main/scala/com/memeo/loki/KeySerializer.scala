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
import com.memeo.loki.IntKey
import com.memeo.loki.BoolKey
import com.memeo.loki.StringKey
import com.memeo.loki.DoubleKey
import com.memeo.loki.ObjectKey
import scala.Serializable
import com.memeo.loki.ArrayKey
import collection.mutable.ArrayBuffer
import java.math.BigInteger
import collection.mutable

object KeySerializer
{
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

class KeySerializer extends BTreeKeySerializer[Key]
{
  import KeySerializer._

  def serializeKey(out:DataOutput, key:Key):Unit = {
    case NullKey => out.write(TypeNull)
    case BoolKey(false) => out.write(TypeFalse)
    case BoolKey(true) => out.write(TypeTrue)
    case i:IntKey => {
      if (i.value.isValidInt)
      {
        out.write(TypeInt)
        out.writeInt(i.value.toInt)
      }
      else
      {
        val value = i.value.underlying().toByteArray
        out.write(TypeBigInt)
        out.writeInt(value.length)
        out.write(value)
      }
    }
    case d:DoubleKey => {
      out.write(TypeDouble)
      out.writeDouble(d.value)
    }
    case s:StringKey => {
      val value = s.value.getBytes(utf8)
      out.write(TypeString)
      out.writeInt(value.length)
      out.write(value)
    }
    case a:ArrayKey => {
      out.write(TypeArray)
      a.value.foreach(e => serialize(out, start, end, Array(e)))
      out.write(TypeEnd)
    }
    case o:ObjectKey => {
      out.write(TypeObject)
      o.value.foreach(e => {
        out.write(TypeArray)
        val key = e._1.getBytes(utf8)
        out.write(TypeString)
        out.writeInt(key.length)
        out.write(key)
        serializeKey(out, e._2)
        out.write(TypeEnd)
      })
      out.write(TypeEnd)
    }
  }

  def serialize(out: DataOutput, start: Int, end: Int, keys: Array[AnyRef]):Unit = {
    out.write(TypeKeys)
    keys.slice(start, end).foreach(k => k match {
      case k:Key => serializeKey(k)
      case s:Serializable => {
        val b = new ByteArrayOutputStream()
        val o = new ObjectOutputStream(b)
        o.writeObject(s)
        o.close()
        val ser = b.toByteArray
        out.write(TypeSerializable)
        out.writeInt(ser.length)
        out.write(ser)
      }
      case null => {
        out.write(TypeNullRef)
      }
    })
    out.write(TypeEnd)
  }

  def deserializeKey(in: DataInput): AnyRef = {
    deserializeKey(in, in.readByte())
  }

  def deserializeArray(in: DataInput, buf:ArrayBuffer[AnyRef]):Unit = {
    in.readByte() match {
      case TypeEnd => Unit
      case x => {
        buf += deserializeKey(in, x)
        deserializeArray(in, buf)
      }
    }
  }

  def deserializeObject(in: DataInput, map:mutable.LinkedHashMap[String, AnyRef]):Unit = {
    in.readByte() match {
      case TypeEnd => Unit
      case TypeArray => {
        if (in.readByte() != TypeString)
          throw new IOException("invalid type tag (expecting string for object key)")
        val key = deserializeKey(in, TypeString).asInstanceOf[StringKey].value
        val value = deserializeKey(in)
        if (in.readByte() != TypeEnd)
          throw new IOException("invalid type tag (expecting end of sequence)")
        map += (key -> value)
        deserializeObject(in, map)
      }
      case _ => throw new IOException("invalid type tag")
    }
  }

  def deserializeKey(in: DataInput, t:Byte) = {
    t match {
      case TypeNull => NullKey
      case TypeFalse => BoolKey(false)
      case TypeTrue => BoolKey(true)
      case TypeInt => {
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFloat(v)
        IntKey(BigInt(new BigInteger(v)))
      }
      case TypeDouble => {
        DoubleKey(in.readDouble())
      }
      case TypeInt => {
        IntKey(BigInt(in.readInt()))
      }
      case TypeBigInt => {
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        IntKey(BigInt(new BigInteger(v)))
      }
      case TypeString => {
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        StringKey(new String(v, utf8))
      }
      case TypeArray => {
        val buf = new ArrayBuffer[AnyRef]()
        deserializeArray(in, buf)
        ArrayKey(buf.toArray[Key])
      }
      case TypeObject => {
        val map = new mutable.LinkedHashMap[String, AnyRef]()
        deserializeObject(in, map)
        ObjectKey(map.toMap[String, Key])
      }
      case TypeSerializable => {
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        val i = new ObjectInputStream(new ByteArrayInputStream(v))
        i.readObject()
      }
    }
  }

  def deserialize(in: DataInput, start: Int, end: Int, size: Int): Array[AnyRef] = {
    val buf = new ArrayBuffer[AnyRef](size)
    Range(0, start, 1).foreach(_ => buf += null)
    Range(start, end, 1).foreach(_ => buf += deserializeKey(in))
    Range(end, size, 1).foreach(_ => buf += null)
    buf.toArray
  }
}
