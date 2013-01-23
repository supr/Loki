package com.memeo.loki

import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.{DefaultFormats, compact, JsonParser}
import org.mapdb.DBMaker
import akka.actor.{Props, Actor}
import akka.pattern.{ask, pipe}
import java.io.{IOError, FilenameFilter, File}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import akka.zeromq.{Frame, ZMQMessage, Connect, ZeroMQExtension}
import concurrent.{Future, future}
import concurrent.duration._
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import akka.util.Timeout
import akka.event.Logging

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
class LokiService(val dbDir:File, val config:ClusterConfig) extends Actor
{
  val logger = Logging(context.system, classOf[LokiService])
  def listDbs():List[String] = dbDir.list(new FilenameFilter {
    def accept(dir: File, name: String): Boolean = name.endsWith(".ldb")
  }).map(name => name.substring(0, name.length - 4)).toList

  def zReq(req:Request):ZMQMessage = {
    ZMQMessage(List(Frame(compact(render(JArray(List(JString(req.method.getName), JString(req.name), req.params, req.headers, req.value)))))))
  }

  def zRes(rez:ZMQMessage):Response = {
    JsonParser.parse(rez.firstFrameAsString) match {
      case JArray(List(code:JInt, headers:JObject, value:JValue)) =>
        Response(HttpResponseStatus.valueOf(code.values.intValue), value, headers)
    }
  }

  import context.dispatcher
  implicit val timeout = Timeout(30 seconds)

  def receive = {
    case (ctx:Any, request:Request) => {
      try {
        request.name.split('/').toList.filter(p => p.length > 0) match {

          // Root, server info.
          case List() => request.method match {
            case GET => {
              val s:Self = config.peers.values.find(m => m.isInstanceOf[Self]).get.asInstanceOf[Self]
              sender ! (ctx, Response(OK, ("version" -> "0.0.1-SNAPSOT") ~ ("peer" -> JInt(s.id)) ~ ("n" -> JInt(config.n)) ~ ("i" -> JInt(config.i)) ~ Nil, JObject(List())))
            }
            case _ => sender ! (ctx, Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
          }

          // List all DBs.
          case List("_all_dbs") => request.method match {
            case GET => {
              config.peers.values.foldLeft[Future[List[String]]](future { List[String]() })((f:Future[List[String]], m) => {
                f.flatMap(l => m match {
                  case s:Self => {
                    logger.info("_all_dbs query self")
                    future {
                      val x = listDbs()
                      logger.info("_all_dbs local list {}", x)
                      l ++ x
                    }
                  }
                  case p:Peer => {
                    logger.info("_all_dbs query peer {} {}", p.id, p.ipcAddr)
                    if (request.params \ "recurse" != JBool(false))
                    {
                      val nextReq = Request(request.method, request.name, JNothing, request.headers,
                        request.params ~ ("recurse" -> JBool(false)))
                      implicit val formats = DefaultFormats
                      implicit val system = context.system
                      context.system.actorOf(Props(new RemoteLoki(p))) ? nextReq map {
                        x => x match {
                          case r:Response => r.value match {
                            case a:JArray => l ++ a.extract[List[String]]
                          }
                        }
                      }
                    }
                    else future { l }
                  }
                })
              }) map {
                l => {
                  logger.info("_all_dbs collected {}", l)
                  (ctx, Response(OK, JArray(l.map(JString)), JObject(List())))
                }
              } pipeTo sender
            }
            case _ => sender ! (ctx, Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
          }

          // Access values for a single container.
          case List(name:String) => request.method match {
            // Get info about a container.
            case GET => {
              config.peers.get(Lookup.hash(name, config.n, config.i)) match {
                case s:Some[Member] => s.get match {
                  case s:Self => {
                    val dbFile = new File(dbDir, name + ".ldb")
                    if (!dbFile.exists()) {
                      sender ! (ctx, Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil, JObject(List())))
                    } else {
                      val container = DBMaker.newRandomAccessFileDB(dbFile).readOnly().make()
                      try
                      {
                        val db = container.getTreeMap[String, Value]("_main", true)
                        sender ! (ctx, Response(OK, ("db_name" -> JString(name)) ~ ("disk_size" -> JInt(dbFile.length())) ~ ("doc_count" -> JInt(db.size())) ~ Nil, JObject(List())))
                      }
                      finally
                      {
                        container.close()
                      }
                    }
                  }
                  case p:Peer => {
                    implicit val system = context.system
                    context.system.actorOf(Props(new RemoteLoki(p))) ? request pipeTo sender
                  }
                }
              }
            }
            case PUT => {
              config.peers.get(Lookup.hash(name, config.n, config.i)) match {
                case s:Some[Member] => s.get match {
                  case s:Self => {
                    val dbFile = new File(dbDir, name + ".ldb")
                    val container = DBMaker.newRandomAccessFileDB(dbFile).make()
                    try
                    {
                      val m = container.createTreeMap[String, Value]("_main", 8, true, null, null, new StringComparator)
                      //m.put("_version", "1")
                      container.commit()
                      sender ! (ctx, Response(OK, ("result" -> JString("OK")) ~ ("created" -> JBool(true)) ~ Nil, JObject(List())))
                    }
                    catch
                    {
                      case iae:IllegalArgumentException => {
                        sender ! (ctx, Response(PRECONDITION_FAILED,
                          ("result" -> JString("error")) ~ ("reason" -> JString("the database already exists")) ~ Nil,
                          JObject(List())))
                      }
                    }
                    finally
                    {
                      container.close()
                    }
                  }
                  case p:Peer => {
                    implicit val system = context.system
                    system.actorOf(Props(new RemoteLoki(p))) ? request pipeTo sender
                  }
                }
              }
            }
            case DELETE => {
              dbDir.listFiles(new FilenameFilter {
                def accept(dir: File, fname: String): Boolean = fname.startsWith(name + ".ldb")
              }).foreach(f => f.delete())
              sender ! (ctx, Response(OK, ("result" -> JString("ok")) ~ Nil, JObject(List())))
            }
            case _ => sender ! (ctx, Response(METHOD_NOT_ALLOWED, JNothing, JObject(List())))
          }

          case List(dbname:String, "_design", dname:String) => {
            sender ! (ctx, Response(NOT_IMPLEMENTED, JNothing, JObject(List())))
          }

          // A document in a container.
          case List(dbname:String, docname:String) => request.method match {
            case GET => {
              config.peers(Lookup.hash(dbname, config.n, config.i)) match {
                case s:Self => {
                  val dbfile = new File(dbDir, dbname + ".ldb")
                  if (!dbfile.exists())
                  {
                    sender ! (ctx, Response(NOT_FOUND,
                                            ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil,
                                            JObject(List())))
                  }
                  else
                  {
                    val container = DBMaker.newRandomAccessFileDB(dbfile).readOnly().make()
                    try
                    {
                      val db = container.getTreeMap[String, Value]("_main")
                      db.get(docname) match {
                        case null =>
                          sender ! (ctx, Response(NOT_FOUND,
                            ("result" -> JString("error")) ~ ("reason" -> JString("not_found")) ~ Nil,
                            JObject(List())))
                        case v:Value => {
                          sender ! (ctx, Response(OK, v.toObject, JObject(List())))
                        }
                      }
                    }
                    finally
                    {
                      container.close()
                    }
                  }
                }
                case p:Peer => {
                  implicit val system = context.system
                  system.actorOf(Props(new RemoteLoki(p))) ? request pipeTo sender
                }
              }
            }
            case PUT => {
              config.peers(Lookup.hash(dbname, config.n, config.i)) match {
                case s:Self => {

                }
                case p:Peer => {
                  implicit val system = context.system
                  system.actorOf(Props(new RemoteLoki(p))) ? request pipeTo sender
                }
              }
            }
          }
          case _ => sender ! (ctx, Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("not found")) ~ Nil,
            JObject(List())))
        }
      } catch {
        case e:Exception => {
          logger.warning("exception handling request {}", e)
          e.printStackTrace()
          sender ! (ctx, Response(INTERNAL_SERVER_ERROR, ("result" -> JString("error")) ~ ("reason" -> JString(e.toString)) ~ Nil, JObject(List())))
        }
        case e:IOError => {
          logger.warning("error handling request {}", e)
          e.printStackTrace()
          sender ! (ctx, Response(INTERNAL_SERVER_ERROR, ("result" -> JString("error")) ~ ("reason" -> JString(e.toString)) ~ Nil, JObject(List())))
        }
      }
    }
  }
}
