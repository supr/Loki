package com.memeo.loki

import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.{DefaultFormats, compact, JsonParser}
import org.mapdb.{DB, DBMaker}
import akka.actor.{Props, Actor}
import akka.pattern.{ask, pipe}
import java.io.{IOError, FilenameFilter, File}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import concurrent.{Future, future}
import concurrent.duration._
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import akka.util.Timeout
import akka.event.Logging
import collection.mutable
import java.math.BigInteger
import java.net.URLDecoder
import collection.convert.WrapAsScala
import collection.mutable.ListBuffer

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
  }).map(name => name.substring(0, name.length - 4).replace(":", "/")).toList

  import context.dispatcher
  implicit val timeout = Timeout(30 seconds)
  implicit val dbdir = dbDir

  def receive = {
    case request:Request => {
      try {
        request.name.split('/').toList.filter(p => p.length > 0).map(e => URLDecoder.decode(e, "UTF-8")) match {

          // Root, server info.
          case List() => request.method match {
            case GET => {
              val s:Self = config.peers.values.find(m => m.isInstanceOf[Self]).get.asInstanceOf[Self]
              sender ! Response(OK, ("version" -> "0.0.1-SNAPSOT") ~ ("peer" -> JInt(s.id)) ~ ("n" -> JInt(config.n)) ~ ("i" -> JInt(config.i)) ~ Nil, JObject(List()))
            }
            case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing, JObject(List()))
          }

          // List all DBs.
          case List("_all_dbs") => request.method match {
            case GET => {
              logger.info("params: {}", request.params)
              Future.sequence(config.peers.values.map(m => m match {
                case s:Self => {
                  logger.info("_all_dbs query self dir:{}", dbDir)
                  future {
                    val x = listDbs()
                    logger.info("_all_dbs local list {}", x)
                    x
                  }
                }
                case p:Peer => {
                  logger.info("_all_dbs query peer {} {} {}", p.id, p.ipcAddr, request.params \ "recurse")
                  request.params \ "recurse" match {
                    case JBool(false) => {
                      logger.info("skip calling peer {}", p.ipcAddr)
                      future { List[String]() }
                    }
                    case x => {
                      logger.info("recursively calling peers because {}", x)
                      val nextReq = Request(request.method, request.name, JNull, request.headers,
                        request.params ~ ("recurse" -> JBool(false)))
                      logger.info("next params {}", nextReq.params)
                      implicit val formats = DefaultFormats
                      context.system.actorFor(p.ipcAddr) ? nextReq map {
                        x => x match {
                          case r:Response => r.value match {
                            case a:JArray => a.extract[List[String]]
                          }
                        }
                      }
                    }
                  }
                }
              })) map {
                ll => {
                  val dblist = JArray((for (l <- ll; e <- l) yield JString(e)).toList)
                  Response(OK, dblist, JObject(List()))
                }
              } pipeTo sender
            }
            case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing, JObject(List()))
          }

          // Access values for a single container.
          case List(name:String) => request.method match {
            // Get info about a container.
            case GET => {
              config.peers.get(Lookup.hash(name, config.n, config.i)) match {
                case s:Some[Member] => s.get match {
                  case s:Self => {
                    Databases().snapshot(name) match {
                      case None =>
                        sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil, JObject(List()))
                      case d:Some[DB] => {
                        val container = d.get
                        val db = container.getTreeMap[String, Value]("_main")
                        logger.info("db={} version={}", db, db.get("_version"))
                        sender ! Response(OK, ("db_name" -> JString(name))
                                              ~ ("disk_size" -> JInt(new File(dbDir, name.replace("/", ":")).length()))
                                              ~ ("doc_count" -> JInt(db.headMap("_").size()))
                                              ~ ("peer" -> JInt(s.id))
                                              ~ Nil, JObject(List()))
                      }
                    }
                  }
                  case p:Peer => {
                    context.system.actorFor(p.ipcAddr) ? request pipeTo sender
                  }
                }
              }
            }
            case PUT => {
              config.peers.get(Lookup.hash(name, config.n, config.i)) match {
                case s:Some[Member] => s.get match {
                  case s:Self => {
                    if (!name.matches("[a-z][-a-z0-9_\\$()+/]*"))
                    {
                      sender ! Response(BAD_REQUEST, ("result" -> JString("error")) ~ ("reason" -> JString("invalid_db_name")) ~ Nil)
                    }
                    else
                    {
                      Databases().get(name, true) match {
                        case None =>
                          sender ! Response(PRECONDITION_FAILED,
                            ("result" -> JString("error")) ~ ("reason" -> JString("db_exists")) ~ Nil,
                            JObject(List()))
                        case d:Some[DB] => {
                          val container = d.get
                          try
                          {
                            val m = container.createTreeMap[String, Value]("_main", 8, true, null, null, new StringComparator)
                            val version = new java.util.LinkedHashMap[String, Object]()
                            version.put("v", 1:Integer)
                            m.put("_version", new Value("_version", BigInteger.valueOf(0), false, version))
                            container.commit()
                            sender ! Response(OK, ("result" -> JString("OK")) ~ ("created" -> JBool(true)) ~ ("peer" -> JInt(s.id)) ~ Nil, JObject(List()))
                          }
                          catch
                          {
                            case iae:IllegalArgumentException => {
                              sender ! Response(PRECONDITION_FAILED,
                                ("result" -> JString("error")) ~ ("reason" -> JString("db_exists")) ~ Nil,
                                JObject(List()))
                            }
                          }
                        }
                      }
                    }
                  }
                  case p:Peer => {
                    context.system.actorFor(p.ipcAddr) ? request pipeTo sender
                  }
                }
              }
            }
            case DELETE => {
              config.peers.get(Lookup.hash(name, config.n, config.i)) match {
                case s:Self => {
                  Databases().delete(name)
                  sender ! Response(OK, ("result" -> JString("ok")) ~ ("peer" -> JInt(s.id)) ~ Nil, JObject(List()))
                }
                case p:Peer => {
                  context.system.actorFor(p.ipcAddr) ? request pipeTo sender
                }
              }
            }
            case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing, JObject(List()))
          }

          case List(dbname:String, "_all_docs") => request.method match {
            case GET => config.peers(Lookup.hash(dbname, config.n, config.i)) match {
              case s:Self => {
                Databases().snapshot(dbname) match {
                  case None =>
                    sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil, JObject(List()))
                  case d:Some[DB] =>
                    val container = d.get
                    val db:Map[String, Value] = WrapAsScala.mapAsScalaMap(container.getTreeMap[String, Value]("_main").headMap(request.params \ "startkey" match {
                      case s:JString => s.values
                      case _ => "_"
                    }).tailMap(request.params \ "endkey" match {
                      case s:JString => s.values
                      case _ => ""
                    })).toMap

                    val results:List[JObject] = db.filter(e => !e._2.deleted).slice(0, request.params \ "limit" match {
                      case s:JString => Math.min(db.size, Integer.parseInt(s.values))
                      case i:JInt => Math.min(db.size, i.values.intValue())
                      case _ => db.size
                    }).foldLeft(new ListBuffer[JObject]())((b:ListBuffer[JObject], e) => {
                      b += {
                        request.params \ "include_docs" match {
                          case JString("true")|JBool(true) =>
                            ("id" -> JString(e._1)) ~ ("rev" -> JString(e._2.genRev)) ~ ("doc" -> e._2.toObject()) ~ Nil
                          case _ =>
                            ("id" -> JString(e._1)) ~ ("rev" -> JString(e._2.genRev)) ~ Nil
                        }
                      }
                    }).toList
                    sender ! Response(OK, ("results" -> JArray(results)))
                  }
                }
              case p:Peer => {
                context.system.actorFor(p.ipcAddr) ? request pipeTo sender
              }
            }
            case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing)
          }

          case List(dbname:String, "_design", dname:String) => {
            sender ! Response(NOT_IMPLEMENTED, JNothing, JObject(List()))
          }

          // A document in a container.
          case List(dbname:String, docname:String) => request.method match {
            case GET => {
              config.peers(Lookup.hash(dbname, config.n, config.i)) match {
                case s:Self => {
                  Databases().snapshot(dbname) match {
                    case None =>
                      sender ! Response(NOT_FOUND,
                                        ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil,
                                        JObject(List()))
                    case d:Some[DB] => {
                      val container = d.get
                      val db = container.getTreeMap[String, Value]("_main")
                      db.get(docname) match {
                        case null =>
                          sender ! Response(NOT_FOUND,
                                            ("result" -> JString("error")) ~ ("reason" -> JString("not_found")) ~ Nil,
                                            JObject(List()))
                        case v:Value => {
                          sender ! Response(OK, v.toObject, ("Loki-Peer-Id" -> JString(s.id.toString)) ~ Nil)
                        }
                      }
                    }
                  }
                }
                case p:Peer => {
                  context.system.actorFor(p.ipcAddr) ? request pipeTo sender
                }
              }
            }
            case PUT => {
              config.peers(Lookup.hash(dbname, config.n, config.i)) match {
                case s:Self => {
                  Databases().get(dbname, false) match {
                    case None =>
                      sender ! Response(NOT_FOUND,
                        ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil,
                        JObject(List()))
                    case d:Some[DB] => {
                      val container = d.get
                      val db = container.getTreeMap[String, Value]("_main")
                      val old = Option(db.get(docname))
                      val value = Value(docname, old match {
                        case v:Some[Value] => v.get.seq.add(BigInteger.ONE)
                        case None => BigInteger.ONE
                      }, request.value)
                      val rev = request.value \ "_rev" match {
                        case s:JString => s.values
                        case _ => ""
                      }
                      if (old.isDefined && old.get.genRev() != rev) {
                        sender ! Response(CONFLICT, ("result" -> JString("error")) ~ ("reason" -> JString("documnent update conflict")) ~ Nil)
                      } else {
                        val replaced = Option(db.put(docname, value))
                        if (replaced.isDefined && replaced.get.genRev() != rev)
                        {
                          container.rollback()
                          sender ! Response(CONFLICT, ("result" -> JString("error")) ~ ("reason" -> JString("documnent update conflict")) ~ Nil)
                        }
                        else
                        {
                          container.commit()
                          sender ! Response(if (old.isDefined) OK else CREATED, ("result" -> JString("ok")) ~ ("rev" -> JString(value.genRev())) ~ ("peer" -> JInt(s.id)) ~ Nil)
                        }
                      }
                    }
                  }
                }
                case p:Peer => {
                  context.system.actorFor(p.ipcAddr) ? request pipeTo sender
                }
              }
            }
          }
          case _ => sender ! Response(NOT_FOUND,
                                      ("result" -> JString("error")) ~ ("reason" -> JString("not found")) ~ Nil,
                                      JObject(List()))
        }
      } catch {
        case e:Exception => {
          logger.warning("exception handling request {}", e)
          e.printStackTrace()
          sender ! Response(INTERNAL_SERVER_ERROR, ("result" -> JString("error")) ~ ("reason" -> JString(e.toString)) ~ Nil, JObject(List()))
        }
        case e:IOError => {
          logger.warning("error handling request {}", e)
          e.printStackTrace()
          sender ! Response(INTERNAL_SERVER_ERROR, ("result" -> JString("error")) ~ ("reason" -> JString(e.toString)) ~ Nil, JObject(List()))
        }
      }
    }
  }
}
