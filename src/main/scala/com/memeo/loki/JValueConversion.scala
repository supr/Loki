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
object JValueConversion
{
  def unpack(value:JValue):Any = {
    value match {
      case JNothing => null
      case JNull => null
      case b:JBool => unpack(b)
      case i:JInt => unpack(i)
      case f:JDouble => java.lang.Double.valueOf(f.values)
      case s:JString => s.values
      case a:JArray => unpack(a)
      case o:JObject => {
        val m = new java.util.LinkedHashMap[String, Any]()
        m.putAll(WrapAsJava.mapAsJavaMap(o.values.map((e) => (e._1 -> unpack(e._2.asInstanceOf[JValue])))))
        m
      }
    }
  }

  def unpack(b:JBool):java.lang.Boolean = {
    java.lang.Boolean.valueOf(b.value)
  }

  def unpack(i:JInt):BigInteger = {
    i.values.underlying()
  }

  def unpack(a:JArray):java.util.List[Any] = {
    new java.util.ArrayList(WrapAsJava.seqAsJavaList(a.children.map(unpack)))
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
}
