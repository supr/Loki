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

import org.junit.Test
import org.junit.Assert._

class TestLookup
{
  def dohash(name: String, n: Int, i: Int):Int = {
    val hash = Lookup.hash(name, n, i)
    System.out.println("hash(" + name + ", " + n + ", " + i + ") = " + hash)
    return hash
  }

  def runit(n:Int, i:Int):Unit = {
    assertTrue(n > dohash("abc", n, i))
    assertTrue(n > dohash("def", n, i))
    assertTrue(n > dohash("ghi", n, i))
    assertTrue(n > dohash("jkl", n, i))
    assertTrue(n > dohash("mno", n, i))
    assertTrue(n > dohash("pqr", n, i))
    assertTrue(n > dohash("stu", n, i))
    assertTrue(n > dohash("vwx", n, i))
    assertTrue(n > dohash("yz", n, i))
    assertTrue(n > dohash("abcdefghijklmnopqrstuvwxyz", n, i))
    assertTrue(n > dohash("a", n, i))
    (1 to 100).foreach({ k:Int => assertTrue(n > dohash(f"a$k%04d", n, i)) })
  }
  
  @Test def test1():Unit = {
    runit(1, 0)
  }

  @Test def test2():Unit = {
    runit(2, 0)
  }

  @Test def test3(): Unit = {
    runit(3, 1)
  }

  @Test def test4(): Unit = {
    runit(4, 1)
  }
}
