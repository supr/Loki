package com.memeo.loki

import net.liftweb.json.JsonAST._
import net.liftweb.json.{JsonParser, compact, render}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import akka.serialization.Serializer
import java.nio.charset.Charset
import java.io.{ByteArrayInputStream, InputStreamReader, IOException}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JArray

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:46 PM
 * To change this template use File | Settings | File Templates.
 */

object Status extends Enumeration
{
  type Status = Value
  val OK = Value(200)
  val CREATED = Value(201)
  val ACCEPTED = Value(202)
  val NO_CONTENT = Value(204)

  val BAD_REQUEST = Value(400)
  val UNAUTHORIZED = Value(401)
  val FORBIDDEN = Value(403)
  val NOT_FOUND = Value(404)
  val METHOD_NOT_ALLOWED = Value(405)
  val CONFLICT = Value(409)
  val PRECONDITION_FAILED = Value(412)

  val INTERNAL_SERVER_ERROR = Value(500)
  val NOT_IMPLEMENTED = Value(501)
}

class Response(val status:Status.Status, val value:JValue, val headers:JObject, val baton:Option[AnyRef])
{
  def toValue():JValue = value match {
    case JNothing => JArray(List(JInt(status.id), headers))
    case _ => JArray(List(JInt(status.id), value, headers))
  }

  override def toString:String = {
    f"com.memeo.loki.Response($status, $value, $headers)"
  }

  def ~~(newBaton:Option[AnyRef]):Response = {
    new Response(status, value, headers, newBaton)
  }
}

object Response
{
  def apply(status:Status.Status, value:JValue, headers:JObject=JObject(List()))(implicit baton:Option[AnyRef]):Response
    = new Response(status, value, headers, baton)

  def apply(resp:Any, newBaton:Option[AnyRef]):Response = resp match {
    case r:Response => new Response(r.status, r.value, r.headers, newBaton)
    case _ => throw new IllegalArgumentException
  }
}

class ResponseSerializer extends Serializer
{
  val utf8 = Charset.forName("UTF-8")

  def identifier: Int = 0x6c6b6902

  def toBinary(o:AnyRef):Array[Byte] = {
    Profiling.begin("serialize_response")
    try {
      if (Main.useJson)
        toBinaryJson(o)
      else
        toBinarySexp(o)
    } finally {
      Profiling.end("serialize_response")
    }
  }

  private def toBinarySexp(o: AnyRef): Array[Byte] = {
    o match {
      case r:Response =>
        SExpSerializer().toBinary(r.toValue())
      case _ => throw new IOException("can't serialize " + o)
    }
  }

  private def toBinaryJson(o:AnyRef):Array[Byte] = o match {
    case r:Response => compact(render(r.toValue())).getBytes(utf8)
    case _ => throw new IOException("can't serialize response")
  }

  def includeManifest: Boolean = false


  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    Profiling.begin("deserialize_response")
    try {
      if (Main.useJson)
        fromBinaryJson(bytes)
      else
        fromBinarySexp(bytes)
    } finally {
      Profiling.end("deserialize_response")
    }
  }

  def fromBinarySexp(bytes: Array[Byte]): AnyRef = {
    implicit val baton:Option[AnyRef] = None
    SExpSerializer().fromBinary(bytes) match {
      case JArray(List(code:JInt, value:JValue, headers:JObject)) =>
        Response(Status(code.values.intValue()), value, headers)
      case JArray(List(code:JInt, headers:JObject)) =>
        Response(Status(code.values.intValue()), JNothing, headers)
      case _ => throw new IOException("malformed response")
    }
  }

  def fromBinaryJson(bytes:Array[Byte]):AnyRef = {
    implicit val baton:Option[AnyRef] = None
    JsonParser.parse(new InputStreamReader(new ByteArrayInputStream(bytes), utf8)) match {
      case JArray(List(code:JInt, value:JValue, headers:JObject)) =>
        Response(Status.apply(code.values.intValue()), value, headers)
      case JArray(List(code:JInt, headers:JObject)) =>
        Response(Status.apply(code.values.intValue()), JNothing, headers)
      case z => throw new IOException("can't deserialize response: " + z)
    }
  }
}