package com.memeo.loki

import org.mapdb.{BTreeMap, DBMaker, DB}
import java.math.BigInteger
import collection.mutable
import java.util
import java.io.File

object MapTest extends App
{
  val db:DB = DBMaker.newFileDB(new File("test.db")).make()
  val map:BTreeMap[String, Value] = db.createTreeMap("main", 32, true, null, null, null)
  var m:java.util.LinkedHashMap[String, Object] = new util.LinkedHashMap[String, Object]()
  m.put("foo", "bar")
  val value1:Value = new Value("a", BigInteger.valueOf(1), false, m)
  map.put("a", value1)
  val value2:Value = new Value("b", BigInteger.valueOf(1), false, m)
  map.put("b", value2)
  val value3:Value = new Value("c", BigInteger.valueOf(1), false, m)
  map.put("c", value3)
  val value4:Value = new Value("d", BigInteger.valueOf(1), false, m)
  map.put("d", value4)
  val value5:Value = new Value("e", BigInteger.valueOf(1), false, m)
  map.put("e", value5)
  val value6:Value = new Value("f", BigInteger.valueOf(1), false, m)
  map.put("f", value6)
  println(map.entrySet())
  println(map.headMap("d").entrySet())
  println(map.tailMap("c").entrySet())
  println(map.subMap("b", "e"))
  println(map.subMap("b", "e").entrySet())
}
