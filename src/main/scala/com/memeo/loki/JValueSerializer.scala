package com.memeo.loki

import org.mapdb.{Serializer, SerializerBase}
import net.liftweb.json.JsonAST._
import collection.convert.{WrapAsScala, WrapAsJava}
import java.math.BigInteger
import scala._
import net.liftweb.json.JsonAST.JDouble
import net.liftweb.json.JsonAST.JBool
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JArray
import java.io.{DataOutput, DataInput}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/18/13
 * Time: 1:30 PM
 * To change this template use File | Settings | File Templates.
 */
class JValueSerializer extends Serializer[JValue]
{
  val defaultSerializer = Serializer.BASIC_SERIALIZER

  def unpack(value:JValue):Any = {
    value match {
      case JNothing => null
      case JNull => null
      case b:JBool => b.value
      case i:JInt => i.values.underlying
      case f:JDouble => f.values
      case s:JString => s.values
      case a:JArray => WrapAsJava.seqAsJavaList(a.children.map(unpack))
      case o:JObject => WrapAsJava.mapAsJavaMap(o.values.map((e) => (e._1 -> unpack(e._2.asInstanceOf[JValue]))))
    }
  }

  def pack(value:Any):JValue = {
    value match {
      case null => JNull
      case b:Boolean => JBool(b)
      case f:Double => JDouble(f)
      case i:BigInteger => JInt(BigInt(i))
      case s:String => JString(s)
      case l:java.util.List[Any] => JArray(WrapAsScala.iterableAsScalaIterable(l).map(pack).toList)
      case m:java.util.Map[String, Any] => JObject(WrapAsScala.iterableAsScalaIterable(m.entrySet).map((e:java.util.Map.Entry[String, Any]) => JField(e.getKey, pack(e.getValue))).toList)
    }
  }

  def serialize(out: DataOutput, value: JValue):Unit = {
    defaultSerializer.serialize(out, unpack(value).asInstanceOf[Object])
  }

  def deserialize(in: DataInput, available: Int): JValue = {
    pack(defaultSerializer.deserialize(in, available))
  }
}
