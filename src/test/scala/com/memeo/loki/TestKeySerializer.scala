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

import junit.framework.Assert._
import junit.framework.TestCase
import junit.framework.Test
import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream}

class TestKeySerializer extends TestCase
{
  def inout(keys:Array[Key]):Array[Key] = {
    inout(keys, 0, keys.length)
  }

  def inout(keys:Array[Key], start:Int, end:Int):Array[Key] = {
    val bout = new ByteArrayOutputStream()
    val dout = new DataOutputStream(bout)
    val serial = new KeySerializer()
    serial.serialize(dout, start, end, keys)
    val din = new DataInputStream(new ByteArrayInputStream(bout.toByteArray))
    serial.deserialize(din, start, end)
  }

  @Test def testNull() = {
    val v1 = NullKey
    val v2 = inout(Array(v1))(0)
    assertEquals(v1, v2)
  }
}
