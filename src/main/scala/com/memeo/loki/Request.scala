package com.memeo.loki

import net.liftweb.json.JsonAST._
import net.liftweb.json.{JsonParser, compact, render}
import org.jboss.netty.handler.codec.http.HttpMethod
import akka.serialization.Serializer
import java.nio.charset.Charset
import java.io.{IOException, ByteArrayInputStream, InputStreamReader}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
class Request(val method:HttpMethod, val name:String, val value:JValue, val headers:JObject, val params:JObject)
{
  def toValue:JValue = {
    JArray(List(JString(method.getName), JString(name), value, headers, params))
  }
}

object Request
{
  def apply(method:HttpMethod, name:String, value:JValue, headers:JObject, params:JObject):Request =
    new Request(method, name, value, headers, params)

  def apply(method:String, name:String, value:JValue, headers:JObject, params:JObject):Request =
    Request(HttpMethod.valueOf(method), name, value, headers, params)
}

class RequestSerializer extends Serializer
{
  val utf8 = Charset.forName("UTF-8")
  def identifier: Int = 0x6c6b6901

  def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case r:Request => {
        compact(render(r.toValue)).getBytes(utf8)
      }
      case _ => throw new IOException("can't serialize request")
    }
  }

  def includeManifest: Boolean = false

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    JsonParser.parse(new InputStreamReader(new ByteArrayInputStream(bytes))) match {
      case JArray(List(method:JString, name:JString, value:JValue, headers:JObject, params:JObject)) =>
        Request(HttpMethod.valueOf(method.values), name.values, value, headers, params)
      case JArray(List(method:JString, name:JString, headers:JObject, params:JObject)) =>
        Request(HttpMethod.valueOf(method.values), name.values, JNothing, headers, params)
      case z => throw new IOException("can't deserialize request: " + z)
    }
  }
}