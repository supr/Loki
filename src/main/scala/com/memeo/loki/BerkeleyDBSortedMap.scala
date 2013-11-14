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

import scala.collection.{SortedMap, concurrent}
import com.sleepycat.je
import java.util.Comparator
import com.sleepycat.je.{CursorConfig, LockMode, OperationStatus, DatabaseEntry}
import com.google.common.cache.{LoadingCache, CacheLoader, CacheBuilder}

class BerkeleyDBSortedMap(val db:je.Database) extends concurrent.Map[Key, Value] with SortedMap[Key, Value]
{
  private val keySerializer = new KeySerializer
  private val valueSerializer = new ValueSerializer

  private val keyCache:LoadingCache[Key, Array[Byte]] = CacheBuilder
    .newBuilder()
    .maximumSize(1024)
    .build(new CacheLoader[Key, Array[Byte]] {
      def load(key: Key): Array[Byte] = {
        return keySerializer.toBinary(key)
      }
    })

  def +=(kv: (Key, Value)): BerkeleyDBSortedMap = {
    val kb = new DatabaseEntry(keyCache.get(kv._1))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      db.put(txn, kb, new DatabaseEntry(valueSerializer.toBinary(kv._2)))
      txn.commit()
    }
    catch {
      case e:Exception => {
        txn.abort()
        throw e
      }
    }
    this
  }

  def -=(key: Key): BerkeleyDBSortedMap = {
    val kb = new DatabaseEntry(keyCache.get(key))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      db.delete(txn, kb) match {
        case OperationStatus.SUCCESS => txn.commit()
        case _ =>
      }
    }
    catch {
      case e:Exception => {
        txn.abort()
        throw e
      }
    }
    this
  }

  def get(key: Key): Option[Value] = {
    val kb = new DatabaseEntry(keyCache.get(key))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      val value = new DatabaseEntry()
      db.get(txn, kb, value, LockMode.READ_UNCOMMITTED) match {
        case OperationStatus.SUCCESS => {
          (value.getOffset, value.getSize) match {
            case (0, value.getData.length) => Some(valueSerializer.fromBinary(value.getData))
            case (i, j) => Some(valueSerializer.fromBinary(value.getData.slice(i, i+j)))
          }
        }
        case OperationStatus.NOTFOUND => None
        case c:OperationStatus => throw new IllegalArgumentException("got unexpected result code: " + c)
      }
    } finally {
      txn.commit()
    }
  }

  def iterator: Iterator[(Key, Value)] = {
    val cursor = db.openCursor(null, CursorConfig.READ_UNCOMMITTED)
    return new Iterator[(Key, Value)] {
      val currentKey = new DatabaseEntry()
      val currentValue = new DatabaseEntry()
      var currentStatus = cursor.getNext(currentKey, currentValue, LockMode.READ_UNCOMMITTED)

      def hasNext: Boolean = currentStatus == OperationStatus.SUCCESS

      def next(): (Key, Value) = {
        if (currentStatus != OperationStatus.SUCCESS)
          throw new NoSuchElementException
        val key = keySerializer.fromBinary(currentKey.getData.slice(currentKey.getOffset, currentKey.getOffset + currentKey.getSize))
        val value = valueSerializer.fromBinary(currentValue.getData.slice(currentValue.getOffset, currentValue.getOffset + currentValue.getSize))
        currentStatus = cursor.getNext(currentKey, currentValue, LockMode.READ_UNCOMMITTED)
        (key, value)
      }
    }
  }

  def putIfAbsent(k: Key, v: Value): Option[Value] = {
    val kb = new DatabaseEntry(keyCache.get(k))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      val value = new DatabaseEntry()
      db.get(txn, kb, value, LockMode.RMW) match {
        case OperationStatus.SUCCESS => {
          txn.abort()
          (value.getOffset, value.getSize) match {
            case (0, value.getData.length) => Some(valueSerializer.fromBinary(value.getData))
            case (i, j) => Some(valueSerializer.fromBinary(value.getData.slice(i, i+j)))
          }
        }
        case OperationStatus.NOTFOUND => {
          db.put(txn, kb, new DatabaseEntry(valueSerializer.toBinary(v)))
          txn.commit()
          None
        }
        case x:OperationStatus => throw new IllegalArgumentException("got unexpected status: " + x)
      }
    } catch {
      case x:Exception => {
        txn.abort()
        throw x
      }
    }
  }

  def remove(k: Key, v: Value): Boolean = {
    val kb = new DatabaseEntry(keyCache.get(k))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      db.delete(txn, kb) match {
        case OperationStatus.SUCCESS => {
          txn.commit()
          true
        }
        case _ => {
          txn.commit()
          false
        }
      }
    } catch {
      case e:Exception => {
        txn.abort()
        throw e
      }
    }
  }

  def replace(k: Key, oldvalue: Value, newvalue: Value): Boolean = {
    val kb = new DatabaseEntry(keyCache.get(k))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      val value = new DatabaseEntry()
      db.get(txn, kb, value, LockMode.RMW) match {
        case OperationStatus.SUCCESS => {
          val v = (value.getOffset, value.getSize) match {
            case (0, value.getData.length) => valueSerializer.fromBinary(value.getData)
            case (i, j) => valueSerializer.fromBinary(value.getData.slice(i, i+j))
          }
          if (oldvalue.equals(v)) {
            db.put(txn, kb, new DatabaseEntry(valueSerializer.toBinary(v)))
            txn.commit()
            true
          } else {
            txn.abort()
            false
          }
        }
        case OperationStatus.NOTFOUND => {
          db.put(txn, kb, new DatabaseEntry(valueSerializer.toBinary(v)))
          txn.commit()
          true
        }
        case x:OperationStatus => throw new IllegalArgumentException("got unexpected status: " + x)
      }
    } catch {
      case x:Exception => {
        txn.abort()
        throw x
      }
    }
  }

  def replace(k: Key, v: Value): Option[Value] = {
    val kb = new DatabaseEntry(keyCache.get(k))
    val txn = db.getEnvironment.beginTransaction(null, null)
    try {
      val value = new DatabaseEntry()
      db.get(txn, kb, value, LockMode.RMW) match {
        case OperationStatus.SUCCESS => {
          db.put(txn, kb, new DatabaseEntry(valueSerializer.toBinary(v)))
          txn.commit()
          (value.getOffset, value.getSize) match {
            case (0, value.getData.length) => Some(valueSerializer.fromBinary(value.getData))
            case (i, j) => Some(valueSerializer.fromBinary(value.getData.slice(i, i+j)))
          }
        }
        case OperationStatus.NOTFOUND => {
          db.put(txn, kb, new DatabaseEntry(valueSerializer.toBinary(v)))
          txn.commit()
          None
        }
        case x:OperationStatus => throw new IllegalArgumentException("got unexpected status: " + x)
      }
    } catch {
      case x:Exception => {
        txn.abort()
        throw x
      }
    }
  }
}
