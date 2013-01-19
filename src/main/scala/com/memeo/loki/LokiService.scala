package com.memeo.loki

import com.twitter.finagle.Service
import com.twitter.util.{Return, Promise, Future}
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import net.liftweb.json.JsonAST.{JArray, JString, JNothing, JObject}
import net.liftweb.json.JsonDSL._
import org.mapdb.DBMaker

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
class LokiService extends Service[Request, Response]
{
  val maindb = DBMaker.newDirectMemoryDB().make()

  def ret[A](a:A):Future[A] = new Promise[A](Return(a))

  def apply(req:Request):Future[Response] = {
    req.name.split('/').toList.filter(p => p.length > 0) match {
      case List() => req.method match {
        case GET => ret(Response(OK, ("version" -> "0.0.1-SNAPSOT") ~ Nil, JObject(List())))
        case _ => ret(Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
      }
      case List("_all_dbs") => req.method match {
        case GET => {
          val list = JArray(collection.JavaConversions.JMapWrapper(maindb.getNameDir).map(e => JString(e._1)).toList)
          ret(Response(OK, list, JObject(List())))
        }
        case _ => ret(Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
      }
      case _ => ret(Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("not found")) ~ Nil,
        JObject(List())))
    }
  }
}
