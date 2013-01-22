package com.memeo.loki

import org.mapdb.Serializer
import net.liftweb.json.JsonAST._
import java.io.{DataOutput, DataInput}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/18/13
 * Time: 1:44 PM
 * To change this template use File | Settings | File Templates.
 */
class JValueKeySerializer extends Serializer[Array[JValue]]
{
  val valueSerializer = new JValueSerializer

  def serialize(out: DataOutput, value: Array[JValue]) = {
      valueSerializer.serialize(out, JArray(a.toList))
  }

  def deserialize(in: DataInput, available: Int): Array[JValue] = {
    valueSerializer.deserialize(in, available) match {
      case l:JArray => l.children.toArray
      case _ => throw new IllegalArgumentException("can't match deserialized value")
    }
  }
}
