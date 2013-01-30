package com.memeo.loki

import org.mapdb.Serializer
import java.io.{IOException, DataOutput, DataInput}
import java.nio.charset.Charset
import java.math.BigInteger
import collection.mutable

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */

object ValueSerializer
{
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
  val TypeKeys   = 'k'.toByte
  val TypeEnd    = 0.toByte
  val TypeSerializable = 'z'.toByte
  val TypeNullRef = '0'.toByte
}

class ValueSerializer extends Serializer[Value] with Serializable
{
  import ValueSerializer._

  def serialize(out: DataOutput, value: Value) = {

  }

  def deserializeMember(in:DataInput):DocumentMember = {
    deserializeMember(in, in.readByte())
  }

  def deserializeMember(in:DataInput, t:Byte):DocumentMember = {
    t match {
      case TypeNull => NullMember
      case TypeFalse
    }
  }

  def deserializeObject(in:DataInput, map:mutable.LinkedHashMap[String, DocumentMember]):Unit = {
    in.readByte() match {
      case TypeArray => {
        if (in.readByte() != TypeString)
          throw new IOException("invalid type tag (expecting string for object key)")
        val key = deserializeKey(in, TypeString).asInstanceOf[StringKey].value
        val value = deserializeKey(in)
        if (in.readByte() != TypeEnd)
          throw new IOException("invalid type tag (expecting end of sequence)")
        map += (key, value)
        deserializeObject(in, map)
      }
      case TypeEnd => Unit
      case _ => throw new IOException("malformed serialized object (excepting key/value or end)")
    }
  }

  def deserialDoc(in: DataInput):Document = {
    if (in.readByte() != TypeObject)
      throw new IOException("malformed serialized value (expecting begin root object)")
    val m = new mutable.LinkedHashMap[String, DocumentMember]()
    deserialObject(in, m)
    new Document(m.toMap)
  }

  def deserialize(in: DataInput, available: Int): Value = {
    in.readByte() match {
      case EndValue => null
      case BeginValue => {
        if (in.readByte() != ValueId)
          throw new IOException("malformed serialized value (no begin ID)")
        val len = in.readInt()
        val v = new Array[Byte](len)
        in.readFully(v)
        val id = new String(v, utf8)
        if (in.readByte() != SequenceId)
          throw new IOException("malformed serialized value (no begin seq)")
        val len2 = in.readInt()
        val v2 = new Array[Byte](len2)
        in.readFully(v2)
        val seq = BigInt(new BigInteger(v2))
        val deleted = in.readBoolean()
        if (deleted)
        {
          new Value(id, seq, true, new Document(Map()))
        }
        else
        {
          new Value(id, seq, false, deserialDoc(in))
        }
      }
      case _ => throw new IOException("malformed serialized value")
    }
  }
}
