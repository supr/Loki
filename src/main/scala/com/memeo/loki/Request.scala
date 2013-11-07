package com.memeo.loki

import net.liftweb.json.JsonAST._
import net.liftweb.json.{JsonParser, compact, render}
import akka.serialization.Serializer
import java.nio.charset.Charset
import java.io.{IOException, ByteArrayInputStream, InputStreamReader}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import scala.collection.convert.Wrappers.{JSetWrapper, JIterableWrapper}
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
object Method extends Enumeration {
  type Method = Value
  val DELETE, GET, HEAD, POST, PUT = Value

  def of(method:org.glassfish.grizzly.http.Method) = {
    method match {
      case org.glassfish.grizzly.http.Method.DELETE => DELETE
      case org.glassfish.grizzly.http.Method.GET => GET
      case org.glassfish.grizzly.http.Method.HEAD => HEAD
      case org.glassfish.grizzly.http.Method.POST => POST
      case org.glassfish.grizzly.http.Method.PUT => PUT
    }
  }
}

class Request(val method:Method.Value, val name:String, val value:JValue, val headers:JObject, val params:JObject)
{
  def toValue:JValue = {
    JArray(List(JString(method.toString), JString(name), value, headers, params))
  }

  override def toString: String = s"Request($method, '$name', $value, $headers, $params)"
}

object Request
{
  def headers(request:org.glassfish.grizzly.http.server.Request):JObject = {
    JObject(JIterableWrapper(request.getHeaderNames()).map(name => {
      JIterableWrapper(request.getHeaders(name)).map(value => {
        JField(name, JString(value))
      })
    }).foldLeft[ListBuffer[JField]](ListBuffer())((l, f) => {
      l ++= f
    }).result())
  }

  def params(request:org.glassfish.grizzly.http.server.Request):JObject = {
    JObject(JSetWrapper(request.getParameterNames).map(name => {
      JField(name, JString(request.getParameter(name)))
    }).toList)
  }

  def apply(request:org.glassfish.grizzly.http.server.Request, value:JValue) = {
    new Request(Method.of(request.getMethod), request.getRequestURI, value, headers(request), params(request))
  }

  def apply(method:Method.Value, name:String, value:JValue, headers:JObject, params:JObject):Request =
    new Request(method, name, value, headers, params)

  def apply(method:String, name:String, value:JValue, headers:JObject, params:JObject):Request =
    Request(Method.withName(method), name, value, headers, params)
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
        Request(Method.withName(method.values), name.values, value, headers, params)
      case JArray(List(method:JString, name:JString, headers:JObject, params:JObject)) =>
        Request(Method.withName(method.values), name.values, JNothing, headers, params)
      case z => throw new IOException("can't deserialize request: " + z)
    }
  }
}