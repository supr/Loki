package com.memeo.loki

import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.mapdb.DBMaker
import akka.actor.{OneForOneStrategy, Actor}
import java.io.{FilenameFilter, File}
import collection.convert.WrapAsScala
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
class LokiService(val dbDir:File) extends Actor
{
  def listDbs:List[String] = dbDir.list(new FilenameFilter {
    def accept(dir: File, name: String): Boolean = name.endsWith(".ldb")
  }).map(name => name.substring(0, name.length - 4)).toList

  def receive = {
    case (ctx:Any, request:Request) => {
      request.name.split('/').toList.filter(p => p.length > 0) match {
        case List() => request.method match {
          case GET => sender ! (ctx, Response(OK, ("version" -> "0.0.1-SNAPSOT") ~ Nil, JObject(List())))
          case _ => sender ! (ctx, Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
        }

        case List("_all_dbs") => request.method match {
          case GET => {
            val list = JArray(listDbs.map(e => JString(e)).toList)
            sender ! (ctx, Response(OK, list, JObject(List())))
          }
          case _ => sender ! (ctx, Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
        }

        case List(name:String) => request.method match {
          case GET => {
            val dbFile = new File(dbDir, name + ".ldb")
            if (!dbFile.exists())
              sender ! (ctx, Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil, JObject(List())))
            else {
              val container = DBMaker.newFileDB(dbFile).readOnly().make()
              val db = container.getTreeMap("*main", false, JValueTypeManifest())
              sender ! (ctx, Response(OK, ("db_name" -> JString(name)) ~ ("disk_size" -> JInt(dbFile.length())) ~ ("doc_count" -> JInt(db.size())) ~ Nil, JObject(List())))
              container.close()
            }
          }
          case POST => {
            val dbFile = new File(dbDir, name + ".ldb")
            val container = DBMaker.newFileDB(dbFile).make()
            container.createTreeMap[JValue, JValue]("*main", 8, true, new JValueKeySerializer, new JValueSerializer, new ValueComparator, JValueTypeManifest())
            sender ! (ctx, Response(OK, ("result" -> JString("OK")) ~ ("created" -> JBool(true)) ~ Nil, JObject(List())))
            container.close()
          }
          case _ => sender ! (ctx, Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
        }
        case _ => sender ! (ctx, Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("not found")) ~ Nil,
          JObject(List())))
      }
    }
  }
}
