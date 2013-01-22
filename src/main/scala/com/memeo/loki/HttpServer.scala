package com.memeo.loki

import org.jboss.netty.channel._
import akka.actor.{Props, ActorSystem, ActorRef, Actor}
import org.jboss.netty.handler.codec.http._
import net.liftweb.json.JsonAST._
import net.liftweb.json.Printer.compact
import org.jboss.netty.buffer.{ChannelBufferInputStream, ChannelBuffer, ChannelBuffers}
import java.nio.charset.Charset
import net.liftweb.json.JsonParser
import java.io.{PrintWriter, StringWriter, InputStreamReader}
import java.net.URI
import collection.mutable.ListBuffer
import java.net.URLDecoder._
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JField
import java.util
import akka.event.Logging
import net.liftweb.json.JsonAST.JObject
import scala.Some
import net.liftweb.json.JsonAST.JField
import net.liftweb.json.JsonAST.JString

object HttpServer
{
  val utf8 = Charset.forName("UTF-8")
}

class HttpActor extends Actor
{

  def receive = {
    case (upstream:ActorRef, e:MessageEvent, httpReq:HttpRequest, req:Request) => {
      upstream ! ((e, httpReq), req)
    }

    case ((e:MessageEvent, httpReq:HttpRequest), resp:Response) => {
      val hr = new DefaultHttpResponse(HttpVersion.HTTP_1_1, resp.status)
      resp.headers.values.foreach((e) => hr.setHeader(e._1, e._2))
      resp.value match {
        case JNothing => hr.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0)
        case v:JValue => {
          val content = ChannelBuffers.copiedBuffer(compact(render(v)), HttpServer.utf8)
          hr.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes)
          hr.setContent(content)
        }
      }
      val future = e.getChannel.write(hr)
      if (HttpHeaders.isKeepAlive(httpReq))
        future.addListener(ChannelFutureListener.CLOSE)
    }

    case e:ExceptionEvent => {
      val logger = Logging(context.system, classOf[HttpActor])
      logger.error(e.getCause, "exception in HttpServer")
      val hr = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR)
      val s = new StringWriter()
      e.getCause.printStackTrace(new PrintWriter(s))
      val content = ChannelBuffers.copiedBuffer(s.toString, HttpServer.utf8)
      hr.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes)
      hr.setContent(content)
      val future = e.getChannel.write(hr)
      future.addListener(ChannelFutureListener.CLOSE)
    }
  }
}

class HttpServer(val upstream:ActorRef, system:ActorSystem) extends SimpleChannelUpstreamHandler
{
  val httpActor = system.actorOf(Props[HttpActor])
  val logger = Logging(system, classOf[HttpServer])

  def parseQuery(query:String):JObject = {
    Option(query) match {
      case None => JObject(List())
      case s:Some[String] => {
        JObject(s.get.split('&').foldLeft[ListBuffer[JField]](new ListBuffer[JField]())((l, e) => {
          val p = e.split("=")
          l += JField(decode(p(0), "UTF-8"), JString(decode(p(1), "UTF-8")))
        }).result())
      }
    }
  }

  def getHeaders(req:util.List[util.Map.Entry[String, String]]):JObject = {
    JObject(scala.collection.JavaConversions.JListWrapper(req)
      .foldLeft[ListBuffer[JField]](new ListBuffer[JField]())((l, e) => {
      l += JField(e.getKey, JString(e.getValue))
    }).result())
  }

  override def messageReceived(ctx:ChannelHandlerContext, e:MessageEvent) = {
    e.getMessage match {
      case req:HttpRequest => {
        if (req.isChunked) {
          val uri = new URI(req.getUri)
          val partial:Request = Request(req.getMethod, uri.getPath, JNothing, getHeaders(req.getHeaders), parseQuery(uri.getQuery))
          ctx.setAttachment((partial, None))
        } else {
          val value = Option(req.getContent) match {
            case None => JNothing
            case buf:Some[ChannelBuffer] if (buf.get.readableBytes() == 0) => JNothing
            case buf:Some[ChannelBuffer] => {
              JsonParser.parse(new InputStreamReader(new ChannelBufferInputStream(buf.get), HttpServer.utf8))
            }
          }
          val uri = new URI(req.getUri)
          val r:Request = Request(req.getMethod, uri.getPath, value, getHeaders(req.getHeaders), parseQuery(uri.getQuery))
          httpActor ! (upstream, e, req, r)
        }
      }
      case chunk:HttpChunk => {
        val (partial, httpRequest, buffer) = ctx.getAttachment.asInstanceOf[(Request, HttpRequest, Option[ChannelBuffer])]
        if (chunk.isLast)
        {
          val tail = chunk.asInstanceOf[HttpChunkTrailer]
          val value = buffer match {
            case None => JNothing
            case s:Some[ChannelBuffer] if (s.get.readableBytes == 0) => JNothing
            case s:Some[ChannelBuffer] => JsonParser.parse(new InputStreamReader(new ChannelBufferInputStream(s.get), HttpServer.utf8))
          }
          val req = Request(partial.method, partial.name, value, (partial.headers ++ getHeaders(tail.getHeaders)).asInstanceOf[JObject], partial.params)
          httpActor ! (upstream, e, httpRequest, req)
        }
        else
        {
          val next = buffer match {
            case s:Some[ChannelBuffer] => ChannelBuffers.wrappedBuffer(buffer.get, chunk.getContent)
            case None => chunk.getContent
          }
          ctx.setAttachment((partial, next))
        }
      }
    }
  }

  override def exceptionCaught(ctx:ChannelHandlerContext, e:ExceptionEvent) = {
    httpActor ! e
  }
}
