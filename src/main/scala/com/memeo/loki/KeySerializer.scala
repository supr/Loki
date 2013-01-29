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
import java.io.{DataOutput, DataInput}
import java.nio.charset.Charset

object KeySerializer
{
  val utf8 = Charset.forName("UTF-8")

  val TypeNull   = 0.toByte
  val TypeFalse  = 1.toByte
  val TypeTrue   = 2.toByte
  val TypeInt    = 3.toByte
  val TypeDouble = 4.toByte
  val TypeString = 5.toByte
  val TypeArray  = 6.toByte
  val TypeObject = 7.toByte
  val TypeEnd    = 8.toByte
  val TypeSerializable = 9.toByte
}

class KeySerializer extends BTreeKeySerializer[Key]
{
  import KeySerializer._

  def serialize(out: DataOutput, start: Int, end: Int, keys: Array[AnyRef]):Unit = {
    keys.slice(start, end).foreach(k => k match {
      case NullKey => out.write(TypeNull)
      case BoolKey(false) => out.write(TypeFalse)
      case BoolKey(true) => out.write(TypeTrue)
      case i:IntKey => {
        val value = i.value.underlying().toByteArray
        out.write(TypeInt)
        out.writeInt(value.length)
        out.write(value)
      }
      case d:DoubleKey => {
        val value = "%d".format(d.value).getBytes(utf8)
        out.write(TypeDouble)
        out.writeInt(value.length)
        out.write(value)
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
        
      }
    })
  }

  def deserialize(in: DataInput, start: Int, end: Int, size: Int): Array[AnyRef] = ???
}
