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
{
  override def toString: String = "BoolKey(" + value.toString() + ")"

  override def equals(obj: Any): Boolean = obj match {
    case b:BoolKey => value == b.value
    case _ => false
  }

  override def hashCode(): Int = if (value) 1 else 0
}

case class IntKey(val value:BigInt) extends Key with Serializable
{
  override def toString: String = "IntKey(" + value.toString() + ")"

  override def equals(obj: Any): Boolean = obj match {
    case i:IntKey => value.equals(i.value)
    case _ => false
  }

  override def hashCode(): Int = value.hashCode()
}

case class DoubleKey(val value:Double) extends Key with Serializable
{
  override def toString: String = "DoubleKey(" + value.toString() + ")"

  override def equals(obj: Any): Boolean = obj match {
    case d:DoubleKey => value == d.value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode()
}

case class StringKey(val value:String) extends Key with Serializable
{
  override def toString: String = "StringKey(" + value + ")"

  override def equals(obj: Any): Boolean = obj match {
    case s:StringKey => value == s.value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode
}

case class ArrayKey(val value:Array[Key]) extends Key with Serializable
{
  override def toString: String = "ArrayKey(" + value.foldLeft(new StringBuilder("["))((s, e) => s.append(e.toString)).append("]").toString() + ")"

  override def equals(obj: Any): Boolean = obj match {
    case a:ArrayKey => value.size == a.value.size && value.zip(a.value).forall(e => e._1 == e._2)
    case _ => false
  }

  override def hashCode(): Int = value.foldLeft(0)((c, k) => c + k.hashCode())
}

case class ObjectKey(val value:Map[String, Key]) extends Key with Serializable
{
  override def toString: String = "ObjectKey(" + value.foldLeft(new StringBuilder("{"))((s, e) => s.append(e._1).append(" -> ").append(e._2.toString).append(", ")).append("}").toString() + ")"

  override def equals(obj: Any): Boolean = obj match {
    case o:ObjectKey => value.size == o.value.size && value.forall(e => o.value.contains(e._1) && o.value(e._1) == e._2)
    case _ => false
  }

  override def hashCode(): Int = value.foldLeft(0)((c, e) => c + e._1.hashCode + e._2.hashCode())
}