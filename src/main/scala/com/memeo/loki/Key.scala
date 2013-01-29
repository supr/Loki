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

abstract class Key
case object NullKey extends Key with Serializable
case class BoolKey(val value:Boolean) extends Key with Serializable
case class IntKey(val value:BigInt) extends Key with Serializable
case class DoubleKey(val value:Double) extends Key with Serializable
case class StringKey(val value:String) extends Key with Serializable
case class ArrayKey(val value:Array[Key]) extends Key with Serializable
case class ObjectKey(val value:Map[String, Key]) extends Key with Serializable