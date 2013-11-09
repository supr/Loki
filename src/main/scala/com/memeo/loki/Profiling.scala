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

import scala.collection.concurrent.TrieMap
import java.util.Date
import concurrent.duration._
import java.util.concurrent.TimeUnit

object Profiling
{
  var profilingEnabled = true
  val opsMaps = new ThreadLocal[TrieMap[String, Long]]
  val ops = new TrieMap[String, (Long, Duration)]()

  def begin(op:String) = {
    if (profilingEnabled) {
      var tops = opsMaps.get()
      if (tops == null)
      {
        tops = new TrieMap[String, Long]()
        opsMaps.set(tops)
      }
      tops.put(op, System.nanoTime())
    }
  }

  def end(op:String) = {
    if (profilingEnabled)
    {
      val tops = opsMaps.get()
      if (tops != null)
      {
        val begin = tops.get(op)
        if (begin.isDefined) {
          val now = System.nanoTime()
          val elapsed = Duration(now - begin.get, TimeUnit.NANOSECONDS)
          val opSummary:(Long, Duration) = ops.get(op) match {
            case s:Some[(Long, Duration)] => s.get
            case None => (0, 0 millis)
          }
          ops.put(op, (opSummary._1 + 1, opSummary._2 + elapsed))

          val o = ops.get(op).get // we just put it, it must be there
          printf("[PROFILING] %s finished in %s\n", op, elapsed)
          printf("[PROFILING] %s ops total, %s total elapsed, %s average\n", o._1, o._2, o._2 / o._1)
        }
      }
    }
  }
}
