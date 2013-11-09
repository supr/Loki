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

import java.util.{Collections, Comparator}
import com.ibm.icu.text.{RuleBasedCollator, Collator}
import java.math.BigInteger
import java.math
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import akka.event.Logging
import akka.actor.ActorSystem

object KeyComparator
{
  val logger = Logging(ActorSystem("loki"), classOf[KeyComparator])
  val collator:Collator = new RuleBasedCollator("<\ufffe")
}

/**
 * JSON value collation, as is done in http://wiki.apache.org/couchdb/View_collation
 */
class KeyComparator extends Comparator[Key] with Serializable
{
  def compare(k1: Key, k2: Key): Int = {
    val res:Int = k1 match {
      case NullKey => k2 match {
        case NullKey => 0
        case _ => -1
      }

      case BoolKey(false) => k2 match {
        case NullKey => 1
        case BoolKey(false) => 0
        case _ => -1
      }

      case BoolKey(true) => k2 match {
        case NullKey|BoolKey(false) => 1
        case BoolKey(true) => 0
        case _ => -1
      }

      case i1:IntKey => k2 match {
        case NullKey|BoolKey(_) => 1
        case i2:IntKey => i1.value.compare(i2.value)
        case d:DoubleKey => i1.value.compare(BigInt(d.value.toInt))
        case _ => -1
      }

      case d1:DoubleKey => k2 match {
        case NullKey|BoolKey(_) => 1
        case i:IntKey => d1.value.compare(i.value.toDouble)
        case d2:DoubleKey => d1.value.compare(d2.value)
        case _ => -1
      }

      case s1:StringKey => k2 match {
        case NullKey|BoolKey(_)|IntKey(_)|DoubleKey(_) => 1
        case s2:StringKey => KeyComparator.collator.compare(s1.value, s2.value)
        case _ => -1
      }

      case a1:ArrayKey => k2 match {
        case NullKey|BoolKey(_)|IntKey(_)|DoubleKey(_)|StringKey(_) => 1
        case a2:ArrayKey => {
          a1.value.zipAll(a2.value, ObjectKey(Map()), ObjectKey(Map())).foldLeft(0)((res, e) => {
            if (res == 0) compare(e._1, e._2) else res
          })
        }
        case _ => -1
      }

      case o1:ObjectKey => k2 match {
        case NullKey|BoolKey(_)|IntKey(_)|DoubleKey(_)|StringKey(_)|ArrayKey(_) => 1
        case o2:ObjectKey => {
          val res = o1.value.zip(o2.value).foldLeft(0)((res, e) => {
            if (res == 0) KeyComparator.collator.compare(e._1._1, e._2._1) else res
          })
          if (res == 0) o1.value.size.compare(o2.value.size) else res
        }
        case _ => -1 // Should not happen?
      }
    }
    res
  }
}
