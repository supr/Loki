/*
 * Copyright 2013 Memeo, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.memeo.loki

import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.DefaultFormats
import akka.actor.{ActorRef, Actor}
import akka.pattern.{ask, pipe}
import java.io.{IOError, FilenameFilter, File}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import concurrent.{Future, future}
import concurrent.duration._
import akka.util.Timeout
import akka.event.Logging
import java.net.URLDecoder
import collection.convert.WrapAsScala
import collection.mutable.ListBuffer

import com.memeo.loki.Method._
import com.memeo.loki.Status._
import com.google.common.cache.{LoadingCache, CacheLoader, CacheBuilder}

import Util.formatDuration
import com.sleepycat.collections.StoredSortedMap
import java.nio.file.Path

class LokiService(val dbDir:Path, val config:ClusterConfig) extends Actor
{
  val logger = Logging(context.system, classOf[LokiService])
  def listDbs():List[String] = Databases().alldbs

  import context.dispatcher
  implicit val timeout = Timeout(30 seconds)
  implicit val dbdir = dbDir
  implicit val system = context.system

  private val remoteActors:LoadingCache[String, ActorRef] = CacheBuilder
    .newBuilder()
    .concurrencyLevel(1)
    .build(new CacheLoader[String, ActorRef]() {
      override def load(key: String):ActorRef = context.system.actorFor(key)
    })

  def receive = {
    case request:Request => {
      implicit val baton:Option[AnyRef] = request.baton
      logger.info("request baton: {}", baton)
      try {
        request.name.split('/').toList.filter(p => p.length > 0).map(e => URLDecoder.decode(e, "UTF-8")) match {

          // Root, server info.
          case List() => request.method match {
            case GET => {
              val s:Self = config.peers.values.find(m => m.isInstanceOf[Self]).get.asInstanceOf[Self]
              sender ! Response(OK, ("version" -> JString("0.0.1-SNAPSOT"))
                ~ ("peer" -> JInt(s.id))
                ~ ("n" -> JInt(config.n))
                ~ ("i" -> JInt(config.i)) ~ Nil)
            }
            case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing, JObject(List()))
          }

          // List all DBs.
          case List("_all_dbs") => allDbs(request, sender)

          case List("_replica", dbname:String) =>
            replicaDb(request, dbname, sender)

          case List("_replica", dbname:String, docname:String) =>
            replicaDoc(request, dbname, docname, sender)

          case List("_profiling") => request.method match {
            case GET => {
              val result = JObject(Profiling.ops.map((e) => {
                JField(e._1, ("count" -> JInt(e._2._1))
                  ~ ("total" -> JInt(e._2._2.toNanos))
                  ~ ("avg" -> JString(formatDuration(e._2._2 / e._2._1)))
                  ~ ("min" -> JString(formatDuration(e._2._4)))
                  ~ ("max" -> JString(formatDuration(e._2._3)))
                  ~ Nil)
              }).toList)
              sender ! Response(OK, result)
            }
            case _ => sender ! Response(METHOD_NOT_ALLOWED, "error" -> JBool(true))
          }

          // Access values for a single container.
          case List(name:String) => database(request, name, sender)

          case List(dbname:String, "_all_docs") => allDocs(request, dbname, sender)

          case List(dbname:String, "_compact") => compact(request, dbname, sender)

          case List(dbname:String, "_design", dname:String) => {
            sender ! Response(NOT_IMPLEMENTED, JNothing, JObject(List()))
          }

          // A document in a container.
          case List(dbname:String, docname:String) => doc(request, dbname, docname, sender)
          case _ => sender ! Response(NOT_FOUND,
                                      ("result" -> JString("error")) ~ ("reason" -> JString("not found")) ~ Nil,
                                      JObject(List()))
        }
      } catch {
        case e:Exception => {
          logger.warning("exception handling request {}", e)
          e.printStackTrace()
          sender ! Response(INTERNAL_SERVER_ERROR,
            ("result" -> JString("error")) ~ ("reason" -> JString(e.toString)) ~ Nil)
        }
        case e:IOError => {
          logger.warning("error handling request {}", e)
          e.printStackTrace()
          sender ! Response(INTERNAL_SERVER_ERROR,
            ("result" -> JString("error")) ~ ("reason" -> JString(e.toString)) ~ Nil,
            JObject(List()))
        }
      }
    }
  }

  def allDbs(request:Request, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    request.method match {
      case GET => {
        logger.debug("params: {}", request.params)
        Future.sequence(config.peers.values.map(m => m match {
          case s:Self => {
            logger.debug("_all_dbs query self dir:{}", dbDir)
            future {
              val x = listDbs()
              logger.debug("_all_dbs local list {}", x)
              x
            }
          }
          case p:Peer => {
            logger.debug("_all_dbs query peer {} {} {}", p.id, p.ipcAddr, request.params \ "recurse")
            request.params \ "recurse" match {
              case JBool(false) => {
                logger.debug("skip calling peer {}", p.ipcAddr)
                future { List[String]() }
              }
              case x => {
                logger.debug("recursively calling peers because {}", x)
                val nextReq = Request(request.method, request.name, JNull, request.headers,
                  request.params ~ ("recurse" -> JBool(false)))
                logger.debug("next params {}", nextReq.params)
                implicit val formats = DefaultFormats
                remoteActors.get(p.ipcAddr) ? nextReq map {
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
            val dblist = JArray((for (l <- ll; e <- l) yield JString(e)).toList.sortBy(s => s.values))
            Response(OK, dblist, JObject(List()))
          }
        } pipeTo sender
      }
      case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing)
    }
  }

  def compact(request:Request, name:String, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    config.peers(Lookup.hash(name, config.n, config.i)) match {
      case s:Self => {
        request.method match {
          case POST => {
            Databases().get(name, false) match {
              case None =>
                sender ! Response(NOT_FOUND,
                  ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil)
              case m:Some[Database] => {
                m.get.compact()
                sender ! Response(OK, "ok" -> JBool(true))
              }
            }
          }
          case _ => sender ! Response(METHOD_NOT_ALLOWED,
            ("result" -> JString("error")) ~ ("reason" -> JString("method_not_supported")) ~ Nil)
        }
      }
      case p:Peer => {
        remoteActors.get(p.ipcAddr) ? request map { r => Response(r, baton) } pipeTo sender
      }
    }
  }

  def database(request:Request, name:String, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    config.peers(Lookup.hash(name, config.n, config.i)) match {
      case s:Self => {
        request.method match {
          // Get info about a container.
          case GET => {
            Databases().snapshot(name) match {
              case None =>
                sender ! Response(NOT_FOUND,
                  ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil)
              case d:Some[Database] => {
                val container = d.get
                container.get("_main", false) match {
                  case m:Some[StoredSortedMap[Key, Value]] => {
                    val db = WrapAsScala.mapAsScalaMap(m.get)
                    val comp = new KeyComparator()
                    sender ! Response(OK, ("db_name" -> JString(name))
                      ~ ("disk_size" -> JInt(container.diskSize))
                      ~ ("doc_count" -> JInt(db.takeWhile(e => comp.compare(e._1, ObjectKey(Map())) < 0).size))
                      ~ ("peer" -> JInt(s.id))
                      ~ Nil)
                  }
                  case None => sender ! Response(NOT_FOUND,
                    ("result" -> JString("error")) ~ ("reason" -> JString("table_not_found")) ~ Nil)
                }
              }
            }
          }
          case PUT => {
            if (!name.matches("[a-z][-a-z0-9_\\$()+/]*"))
            {
              sender ! Response(BAD_REQUEST,
                ("result" -> JString("error")) ~ ("reason" -> JString("invalid_db_name")) ~ Nil)
            }
            else
            {
              Databases().get(name, true) match {
                case None =>
                  sender ! Response(PRECONDITION_FAILED,
                    ("result" -> JString("error")) ~ ("reason" -> JString("db_exists")) ~ Nil)
                case d:Some[Database] => {
                  val container = d.get
                  try
                  {
                    val m = container.get("_main", true).get
                    val key = new ObjectKey(Map[String, Key]())
                    val value = new Document(Map("version" -> IntMember(BigInt(0))))
                    m.put(key, new Value(key, BigInt(0), false, value, List()))
                    container.commit()
                    if (config.nodeCount >= 3) {
                      val peer0 = config.peers.get(config.prev).get.asInstanceOf[Peer]
                      val peer1 = config.peers.get(config.next).get.asInstanceOf[Peer]
                      val req = Request(PUT, s"/_replica/$name", JNothing,
                        JObject(List()), JObject(List()))
                      for {
                        r1 <- remoteActors.get(peer0.ipcAddr) ? req
                        r2 <- remoteActors.get(peer1.ipcAddr) ? req
                      } yield {
                        sender ! Response(OK,
                          ("result" -> JString("OK"))
                            ~ ("created" -> JBool(true))
                            ~ ("peer" -> JInt(s.id)) ~ Nil)
                      }
                    } else {
                      sender ! Response(OK,
                        ("result" -> JString("OK"))
                          ~ ("created" -> JBool(true))
                          ~ ("peer" -> JInt(s.id)) ~ Nil)
                    }
                    logger.info("created {}", name)
                  }
                  catch
                    {
                      case iae:IllegalArgumentException => {
                        sender ! Response(PRECONDITION_FAILED,
                          ("result" -> JString("error")) ~ ("reason" -> JString("db_exists")) ~ Nil)
                    }
                  }
                }
              }
            }
          }
          case DELETE => {
            Databases().delete(name)
            sender ! Response(OK,
              ("result" -> JString("ok")) ~ ("peer" -> JInt(s.id)) ~ Nil)
          }
          case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing)
        }
      }
      case p:Peer => {
        remoteActors.get(p.ipcAddr) ? request map {r => Response(r, baton)} pipeTo sender
      }
    }
  }

  def allDocs(request:Request, dbname:String, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    config.peers(Lookup.hash(dbname, config.n, config.i)) match {
      case s:Self => request.method match {
        case GET => {
          Databases().snapshot(dbname) match {
            case None =>
              sender ! Response(NOT_FOUND,
                ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil)
            case d:Some[Database] =>
              val container = d.get
              val startkey = request.params \ "startkey" match {
                case JNull|JNothing => NullKey
                case s:JString => StringKey(s.values)
                case _ => throw new IllegalArgumentException("invalid startkey")
              }
              val endkey = request.params \ "endkey" match {
                case JNull|JNothing => ObjectKey(Map())
                case s:JString => StringKey(s.values)
                case _ => throw new IllegalArgumentException("invalid endkey")
              }
              container.get("_main", false) match {
                case None => sender ! Response(NOT_FOUND,
                  ("result" -> JString("error")) ~ ("reason" -> JString("table_not_found")) ~ Nil)
                case m:Some[StoredSortedMap[Key, Value]] => {
                  val db = WrapAsScala.mapAsScalaMap(m.get.subMap(startkey, endkey))
                  val limit:Int = request.params \ "limit" match {
                    case JNull|JNothing => Integer.MAX_VALUE
                    case i:JInt => i.values.toInt
                    case s:JString => Integer.parseInt(s.values)
                    case _ => throw new IllegalArgumentException("invalid limit")
                  }
                  val include_docs = request.params \ "include_docs" match {
                    case JNull|JNothing => false
                    case b:JBool => b.values
                    case s:JString => s.values.toBoolean
                    case _ => false
                  }
                  val comp = new KeyComparator()

                  // TODO we should navigate to the first element, and iterate from there, but
                  // MapDB is buggy with subMaps right now.
                  val buf:ListBuffer[JValue] = new ListBuffer[JValue]()
                  db.takeWhile((e) => buf.size < limit)
                    .filter(e => !e._2.deleted)
                    .foreach(e => {
                      if (include_docs)
                        buf += ("id" -> JValueConversion.packKey(e._1)) ~ ("rev" -> JString(e._2.genRev())) ~ ("doc" -> e._2.toValue()) ~ Nil
                      else
                        buf += ("id" -> JValueConversion.packKey(e._1)) ~ ("rev" -> JString(e._2.genRev())) ~ Nil
                    })
                  sender ! Response(OK, "results" -> JArray(buf.toList))
                }
              }
          }
        }
        case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing)
      }
      case p:Peer => {
        remoteActors.get(p.ipcAddr) ? request map {r => Response(r, baton)} pipeTo sender
      }
    }
  }

  def doc(request:Request, dbname:String, docname:String, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    config.peers(Lookup.hash(dbname, config.n, config.i)) match {
      case s:Self => {
        request.method match {
          case GET => {
            Databases().snapshot(dbname) match {
              case None =>
                sender ! Response(NOT_FOUND,
                  ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil,
                  JObject(List()))
              case d:Some[Database] => {
                val container = d.get
                container.get("_main", false) match {
                  case None =>
                    sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("table_not_found")) ~ Nil)
                  case m:Some[StoredSortedMap[Key, Value]] => {
                    val db = m.get
                    db.get(StringKey(docname)) match {
                      case null =>
                        sender ! Response(NOT_FOUND,
                          ("result" -> JString("error")) ~ ("reason" -> JString("not_found")) ~ Nil,
                          JObject(List()))
                      case v:Value => {
                        sender ! Response(OK, v.toValue, ("Loki-Peer-Id" -> JString(s.id.toString)) ~ Nil)
                      }
                    }
                  }
                }
              }
            }
          }
          case PUT => {
            Databases().get(dbname, false) match {
              case None =>
                sender ! Response(NOT_FOUND,
                  ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil)
              case d:Some[Database] => {
                val container = d.get
                container.get("_main", false) match {
                  case None =>
                    sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("table_not_found")) ~ Nil)
                  case m:Some[StoredSortedMap[Key, Value]] => {
                    val docid = new StringKey(docname)
                    val db = m.get
                    val old = Option(db.get(docid))
                    val value = Value(docid, old match {
                      case v:Some[Value] => v.get.seq + BigInt(1)
                      case None => BigInt(1)
                    }, request.value, old match {
                      case v:Some[Value] => new Revision(v.get.seq, v.get.revStr()) :: v.get.revisions
                      case None => List[Revision]()
                    })
                    val rev = request.value \ "_rev" match {
                      case s:JString => s.values
                      case _ => ""
                    }
                    if (old.isDefined && !old.get.deleted && old.get.genRev() != rev) {
                      sender ! Response(CONFLICT, ("result" -> JString("error")) ~ ("reason" -> JString("documnent update conflict")) ~ Nil)
                    } else {
                      Profiling.begin("put_update")
                      val replaced = Option(db.put(docid, value))
                      Profiling.end("put_update")
                      if (replaced.isDefined && replaced.get.genRev() != rev)
                      {
                        container.rollback()
                        sender ! Response(CONFLICT, ("result" -> JString("error")) ~ ("reason" -> JString("documnent update conflict")) ~ Nil)
                      }
                      else
                      {
                        if (config.nodeCount >= 3) {
                          val peer0 = config.peers.get(config.prev).get.asInstanceOf[Peer]
                          val peer1 = config.peers.get(config.next).get.asInstanceOf[Peer]
                          val req = Request(PUT, s"/_replica/$dbname/$docname",
                            JArray(List(JValueConversion.packKey(value.id),
                              JInt(value.seq),
                              JValueConversion.pack(value.value),
                              JValueConversion.packRevs(value.revisions))),
                            JObject(List()), JObject(List()))
                          for {
                            r1 <- remoteActors.get(peer0.ipcAddr) ? req
                            r2 <- remoteActors.get(peer1.ipcAddr) ? req
                          } yield {
                            container.commit()
                            sender ! Response(if (old.isDefined) OK else CREATED, ("result" -> JString("ok")) ~ ("rev" -> JString(value.genRev())) ~ ("peer" -> JInt(s.id)) ~ Nil)
                          }
                        } else {
                          container.commit()
                          sender ! Response(if (old.isDefined) OK else CREATED, ("result" -> JString("ok")) ~ ("rev" -> JString(value.genRev())) ~ ("peer" -> JInt(s.id)) ~ Nil)
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          case DELETE => {
            Databases().get(dbname, false) match {
              case None =>
                sender ! Response(NOT_FOUND,
                  ("result" -> JString("error")) ~ ("reason" -> JString("no_db_file")) ~ Nil)
              case d:Some[Database] => {
                val container = d.get
                container.get("_main", false) match {
                  case None =>
                    sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("table_not_found")) ~ Nil)
                  case m:Some[StoredSortedMap[Key, Value]] => {
                    val docid = new StringKey(docname)
                    val db = m.get
                    val old = db.get(docid)
                    old match {
                      case null => sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("not found")) ~ Nil)
                      case v:Value if v.deleted => sender ! Response(NOT_FOUND, ("result" -> JString("error")) ~ ("reason" -> JString("deleted")) ~ Nil)
                      case v:Value => request.params \ "rev" match {
                        case s:JString => {
                          if (v.genRev() == s.values) {
                            val newval = new Value(docid, old.seq + 1, true, new Document(Map()), new Revision(old.seq, old.revStr()) :: old.revisions)
                            Profiling.begin("db_update_delete")
                            val replaced = db.put(docid, newval)
                            Profiling.end("db_update_delete")
                            if (old.seq == replaced.seq) {
                              d.get.commit()
                              sender ! Response(OK, ("result" -> "ok") ~ ("rev" -> JString(newval.genRev())) ~ Nil)
                            }
                            else
                            {
                              d.get.rollback()
                              sender ! Response(CONFLICT, ("result" -> JString("error")) ~ ("reason" -> JString("document update conflict")) ~ Nil)
                            }
                          }
                          else
                          {
                            sender ! Response(CONFLICT, ("result" -> JString("error")) ~ ("reason" -> JString("document update conflict")) ~ Nil)
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing)
        }
      }
      case p:Peer => {
        logger.info("forwarding op {}/{} to {}", dbname, docname, p.ipcAddr)
        remoteActors.get(p.ipcAddr) ? request map {r=>Response(r, baton)} pipeTo sender
      }
    }
  }

  def replicaDb(request:Request, dbname:String, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    request.method match {
      case PUT => {
        Databases().get(dbname, true) match {
          case s:Some[Database] => {
            val container = s.get
            container.get("_replica", true) match {
              case s:Some[StoredSortedMap[Key, Value]] => {
                val m = s.get
                val key = new ObjectKey(Map[String, Key]())
                val value = new Document(Map("version" -> IntMember(BigInt(0))))
                m.put(key, new Value(key, BigInt(0), false, value, List()))
                container.commit()
                sender ! Response(OK, "created" -> JBool(true))
              }
              case None => sender ! Response(PRECONDITION_FAILED,
                ("error" -> JBool(true)) ~ ("reason" -> JString("failed_creating_table")) ~ Nil)
            }
          }
          case None => sender ! Response(PRECONDITION_FAILED,
            ("error" -> JBool(true)) ~ ("reason" -> JString("failed_creating_db")) ~ Nil)
        }
      }
      case DELETE => {
        Databases().delete(dbname)
        sender ! Response(OK, ("result" -> JString("ok")) ~ Nil)
      }
      case _ => Response(METHOD_NOT_ALLOWED, JNothing)
    }
  }

  def replicaDoc(request:Request, dbname:String, docname:String, sender:ActorRef)(implicit baton:Option[AnyRef]) = {
    request.method match {
      case GET => {
        Databases().get(dbname, false) match {
          case s:Some[Database] => s.get.get("_replica", false) match {
            case s:Some[StoredSortedMap[Key, Value]] => {
              val key = StringKey(docname)
              Option(s.get.get(key)) match {
                case s:Some[Value] =>
                  sender ! Response(OK, JArray(List(JValueConversion.packKey(key),
                    JInt(s.get.seq), JValueConversion.pack(s.get.value),
                    JValueConversion.packRevs(s.get.revisions))))
                case None =>
                  sender ! Response(NOT_FOUND, ("error" -> true) ~ ("reason" -> "doc_not_found"))
              }
            }
            case None =>
              sender ! Response(NOT_FOUND, ("error" -> true) ~ ("reason" -> "table_not_found"))
          }
          case None =>
            sender ! Response(NOT_FOUND, ("error" -> true) ~ ("reason" -> "no_db_file"))
        }
      }
      case PUT => {
        Databases().get(dbname, false) match {
          case s:Some[Database] => s.get.get("_replica", false) match {
            case s:Some[StoredSortedMap[Key, Value]] => request.value match {
              case JArray(List(jkey:JValue, seq:JInt, doc:JValue, revs:JArray)) => {
                val key = JValueConversion.unpackKey(jkey)
                val value = Value(key, seq.values, doc, JValueConversion.unpackRevs(revs))
                s.get.put(key, value)
                sender ! Response(OK, JNothing)
              }
              case _ =>
                sender ! Response(BAD_REQUEST, ("error" -> true) ~ ("reason" -> "bad_value"))
            }
            case None =>
              sender ! Response(NOT_FOUND, ("error" -> true) ~ ("reason" -> "table_not_found"))
          }
          case None =>
            sender ! Response(NOT_FOUND, ("error" -> true) ~ ("reason" -> "no_db_file"))
        }
      }
      case DELETE =>
      case _ => sender ! Response(METHOD_NOT_ALLOWED, JNothing)
    }
  }
}
