package com.memeo.loki

import org.mapdb.{SerializerPojo, Serializer}
import net.liftweb.json.JsonAST._
import java.io.{DataOutput, DataInput}
import net.liftweb.json.JsonAST.JBool
import java.math.BigInteger

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/18/13
 * Time: 1:44 PM
 * To change this template use File | Settings | File Templates.
 */
class JValueKeySerializer extends Serializer[Array[JValue]]
{
  import collection.JavaConversions.IterableWrapper
  import collection.JavaConversions.JListWrapper
  import collection.JavaConversions.JMapWrapper

  val defaultSerializer = new SerializerPojo()

  def unpack(value:JValue):Any = {
    value match {
      case JNothing => null
      case JNull => null
      case b:JBool => b.value
      case i:JInt => i.values.underlying
      case f:JDouble => f.values
      case s:JString => s.values
      case a:JArray => JListWrapper(a.children.map(unpack))
      case o:JObject => JMapWrapper(o.values.map((e) => (e._1 -> unpack(e._2))))
    }
  }

  def pack(value:Any):JValue = {
    value match {
      case null => JNull
      case b:Boolean => JBool(b)
      case f:Double => JDouble(f)
      case i:BigInteger => JInt(BigInt(i))
      case s:String => JString(s)
      case l:util.List[Any] => JArray(l.map(pack))
      case m:util.Map[String, Any] => JObject(IterableWrapper(m.entrySet).map((e) => JField(e.getKey, pack(e.getValue))))
    }
  }

  def serialize(out: DataOutput, value: Array[JValue]) = {
    defaultSerializer.serialize(out, JListWrapper(value.map(unpack)))
  }

  def deserialize(in: DataInput, available: Int): Array[JValue] = {
    defaultSerializer.deserialize(in, available) match {
      case l:java.lang.Iterable[Any] => collection.JavaConversions.IterableWrapper(l).map(pack).toArray
      case _ => throw new IllegalArgumentException("can't match deserialized value")
    }
  }
}
