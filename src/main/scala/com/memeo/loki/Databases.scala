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
import com.google.common.cache.{RemovalNotification, RemovalListener, Cache, CacheBuilder}
import java.util.concurrent.{ExecutionException, Callable}
import akka.actor.ActorSystem
import com.google.common.util.concurrent.UncheckedExecutionException
import java.util
import com.sleepycat.collections.StoredSortedMap
import com.sleepycat.je
import com.sleepycat.je.{DatabaseConfig, Environment, EnvironmentConfig}

class Database(val env:Environment, val name:String)(implicit val system:ActorSystem)
{
  private val cache:Cache[String, StoredSortedMap[Key, Value]] = CacheBuilder
    .newBuilder()
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .softValues()
    .maximumSize(32)
    .build()

  def get(tableName:String, create:Boolean = true):Option[StoredSortedMap[Key, Value]] = {
    try
    {
      Some(cache.get(tableName, new Callable[StoredSortedMap[Key, Value]]() {
        override def call():StoredSortedMap[Key, Value] = {
          val config = new DatabaseConfig()
          config.setAllowCreate(create)
          config.setBtreeComparator(new KeyComparatorAdapter)
          new StoredSortedMap(env.openDatabase(null, tableName, config),
            new KeyBinding, new ValueBinding, true)
        }
      }))
    }
    catch {
      case e:NoSuchElementException => None
    }
  }

  def snapshot():Database = {
    this
  }

  def compact():Unit = Unit
  def commit():Unit = {
    Profiling.begin("db_commit")
    try {
      env.sync()
    } finally {
      Profiling.end("db_commit")
    }
  }
  def close():Unit = env.close()
  def rollback():Unit = Unit

  def delete():Unit = {
    env.removeDatabase(null, name)
  }
}

class Databases(val dbdir: File)(implicit val system:ActorSystem)
{
  import Databases.dbCache

  def get(name: String, create: Boolean = true): Option[Database] = {
    try {
      Some(dbCache.get(name, new Callable[Database]()
      {
        override def call():Database = {
          val f = new File(dbdir, name.replace("/", ":"))
          if (!f.exists()) {
            if (!create)
              throw new NoSuchElementException
          }
          try {
            Profiling.begin("create_new_db")
            if (!f.mkdirs())
              throw new NoSuchElementException("failed to create directory")
            val envConfig = new EnvironmentConfig()
            envConfig.setAllowCreate(create)
            envConfig.setTransactional(true)
            envConfig.setLocking(true)
            envConfig.setSharedCache(true)
            val env = new Environment(f, envConfig)
            new Database(env, name)
          } finally {
            Profiling.end("create_new_db")
          }
        }
      }))
    }
    catch {
      case e:UncheckedExecutionException if e.getCause.isInstanceOf[NoSuchElementException] => None
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
    val dir = new File(dbdir, name)
    dir.listFiles().foreach(f => f.delete())
    dir.delete()
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

  def apply()(implicit dbdir: File, system:ActorSystem) = new Databases(dbdir)
}
