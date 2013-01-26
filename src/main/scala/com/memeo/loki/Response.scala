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
class Response(val status:HttpResponseStatus, val value:JValue, val headers:JObject)
{
  def toValue():JValue = JArray(List(JInt(status.getCode), value, headers))
}

object Response
{
  def apply(status:HttpResponseStatus, value:JValue, headers:JObject=JObject(List())):Response = new Response(status, value, headers)
}

class ResponseSerializer extends Serializer
{
  val utf8 = Charset.forName("UTF-8")

  def identifier: Int = 0x6c6b6902

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case r:Response => compact(render(r.toValue())).getBytes(utf8)
    case _ => throw new IOException("can't serialize response")
  }

  def includeManifest: Boolean = false

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    JsonParser.parse(new InputStreamReader(new ByteArrayInputStream(bytes), utf8)) match {
      case JArray(List(code:JInt, value:JValue, headers:JObject)) =>
        Response(HttpResponseStatus.valueOf(code.values.intValue()), value, headers)
      case JArray(List(code:JInt, headers:JObject)) =>
        Response(HttpResponseStatus.valueOf(code.values.intValue()), JNothing, headers)
      case z => throw new IOException("can't deserialize response: " + z)
    }
  }
}