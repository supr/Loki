package com.memeo.loki

import collection.mutable.ListBuffer
import com.twitter.finagle.{Filter, Service}
import com.twitter.util.Future
import java.net.URI
import java.net.URLDecoder.decode
import java.nio.charset.Charset
import java.util
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonParser
import net.liftweb.json.Printer.compact
import org.jboss.netty.buffer.{ChannelBufferInputStream, ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http._
import net.liftweb.json.JsonAST.JField
import net.liftweb.json.JsonAST.JObject
import java.io.{InputStreamReader, Reader}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 3:01 PM
 * To change this template use File | Settings | File Templates.
 */
class HttpAdapterFilter extends Filter[HttpRequest, HttpResponse, Request, Response]
{
  val utf8 = Charset.forName("UTF-8")

  def reader(buffer:ChannelBuffer):Reader = new InputStreamReader(new ChannelBufferInputStream(buffer), utf8)

  def parseQuery(query:String):JObject = {
    JObject(query.split('&').foldLeft[ListBuffer[JField]](new ListBuffer[JField]())((l, e) => {
      val p = e.split("=")
      l += JField(decode(p(0), "UTF-8"), JString(decode(p(1), "UTF-8")))
    }).result())
  }

  def getHeaders(req:util.List[util.Map.Entry[String, String]]):JObject = {
    JObject(scala.collection.JavaConversions.JListWrapper(req)
      .foldLeft[ListBuffer[JField]](new ListBuffer[JField]())((l, e) => {
      l += JField(e.getKey, JString(e.getValue))
    }).result())
  }

  override def apply(request:HttpRequest, service:Service[Request, Response]):Future[HttpResponse] = {
    val uri = new URI(request.getUri)
    val req = new Request(request.getMethod, uri.getPath, JsonParser.parse(reader(request.getContent)), getHeaders(request.getHeaders), parseQuery(uri.getQuery))
    val resp:Future[Response] = service(req)
    resp.map(r => {
      val hr = new DefaultHttpResponse(HttpVersion.HTTP_1_1, r.status)
      r.headers.values.foreach(e => hr.addHeader(e._1, e._2.toString))
      r.value match {
        case JNothing => Unit
        case v:JValue => hr.setContent(ChannelBuffers.copiedBuffer(compact(render(v)), utf8))
      }
      hr
    })
  }
}
