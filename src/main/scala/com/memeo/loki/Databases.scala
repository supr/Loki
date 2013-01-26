package com.memeo.loki

import org.mapdb.{DBMaker, DB}
import collection.concurrent.TrieMap
import java.io.{FilenameFilter, File}
import collection.convert.WrapAsScala

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/25/13
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */
class Databases(val dbdir:File)
{
  import Databases.dbs

  def get(name:String, create:Boolean=true):Option[DB] = {
    dbs.get(name) match {
      case s:Some[DB] => {
        if (create) None
        else s
      }
      case None => {
        val f = new File(dbdir, name.replace("/", ":") + ".ldb")
        if (!f.exists()) {
          if (!create) {
            None
          } else {
            val db = DBMaker.newRandomAccessFileDB(new File(dbdir, name.replace("/", ":") + ".ldb")).make()
            dbs.putIfAbsent(name, db) match {
              case s:Some[DB] => {
                db.close()
                s
              }
              case None => Some(db)
            }
          }
        } else {
          if (create) {
            None
          } else {
            val db = DBMaker.newRandomAccessFileDB(new File(dbdir, name.replace("/", ":") + ".ldb")).make()
            dbs.putIfAbsent(name, db) match {
              case s:Some[DB] => {
                db.close()
                s
              }
              case None => Some(db)
            }
          }
        }
      }
    }
  }

  def snapshot(name:String):Option[DB] = {
    get(name, false) match {
      case db:Some[DB] => Some(db.get.snapshot())
      case None => None
    }
  }

  def delete(name:String):Unit = {
    if (dbs.contains(name)) {
      dbs.remove(name)
    }

    dbdir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.startsWith(name.replace("/", ":") + ".ldb")
    }).foreach(f => f.delete())
  }
}

object Databases
{
  private val dbs:collection.concurrent.Map[String, DB] = new TrieMap[String, DB]()

  def apply()(implicit dbdir:File) = new Databases(dbdir)
}
