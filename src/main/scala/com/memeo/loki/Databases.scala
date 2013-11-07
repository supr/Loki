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

import org.mapdb.{BTreeMap, DBMaker, DB}
import collection.concurrent.TrieMap
import java.io.{FilenameFilter, File}
import collection.convert.WrapAsScala
import com.google.common.cache.{Cache, CacheBuilder}
import java.util.concurrent.{ExecutionException, Callable}

class Database(val db:DB, val file:File, val isSnapshot:Boolean = false)
{
  private val cache:Cache[String, BTreeMap[Key, Value]] = CacheBuilder
    .newBuilder()
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .softValues()
    .maximumSize(32)
    .build()

  def get(tableName:String, create:Boolean = true):Option[BTreeMap[Key, Value]] = {
    try{
      Some(cache.get(tableName, new Callable[BTreeMap[Key, Value]]() {
        override def call():BTreeMap[Key, Value] = {
          try {
            db.getTreeMap[Key, Value](tableName)
          }
          catch {
            case e:NoSuchElementException if create => {
              db.createTreeMap(tableName)
                .nodeSize(32)
                .valuesStoredOutsideNodes(true)
                .comparator(new KeyComparator)
                .keySerializer(new KeySerializer)
                .valueSerializer(new ValueSerializer)
                .make[Key, Value]()
            }
          }
        }
      }))
    }
    catch {
      case e:NoSuchElementException => None
    }
  }

  def snapshot():Database = {
    new Database(db.snapshot(), file, true)
  }

  def compact():Unit = db.compact()
  def commit():Unit = db.commit()
  def close():Unit = db.close()
  def rollback():Unit = db.rollback()

  def delete():Unit = {
    db.close()
    val dir = file.getParentFile
    dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.startsWith(file.getName)
    }).foreach(f => f.delete())
  }
}

class Databases(val dbdir: File)
{
  import Databases.dbCache

  def get(name: String, create: Boolean = true): Option[Database] = {
    try {
      Some(dbCache.get(name, new Callable[Database]() {
        override def call():Database = {
          val f = new File(dbdir, name.replace("/", ":") + ".ldb")
          if (!f.exists()) {
            if (!create)
              throw new NoSuchElementException
          }
          new Database(DBMaker.newFileDB(f).strictDBGet().make(), f)
        }
      }))
    }
    catch {
      case e:ExecutionException if e.getCause.isInstanceOf[NoSuchElementException] => None
    }
  }

  def snapshot(name: String): Option[Database] = {
    get(name, false) match {
      case db: Some[Database] => Some(db.get.snapshot())
      case None => None
    }
  }

  def delete(name: String): Unit = {
    dbCache.invalidate(name)

    dbdir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = name.startsWith(name.replace("/", ":") + ".ldb")
    }).foreach(f => f.delete())
  }
}

object Databases
{
  private val dbCache:Cache[String, Database] = CacheBuilder
    .newBuilder()
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .softValues()
    .maximumSize(1024)
    .build()

  def apply()(implicit dbdir: File) = new Databases(dbdir)
}
