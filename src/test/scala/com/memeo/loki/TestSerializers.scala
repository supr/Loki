package com.memeo.loki

import org.junit.Test
import org.junit.Assert._
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonDSL._

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
class TestSerializers
{
  @Test def test1() = {
    val request = Request(Method.GET, "/foo/bar", JNull, JObject(List()), JObject(List()))
    val bytes = new RequestSerializer().toBinary(request)
    println("got serialized request: " + new String(bytes))
    val result = new RequestSerializer().fromBinary(bytes)
    assertTrue(result.isInstanceOf[Request])
    val request2 = result.asInstanceOf[Request]
    assertEquals(request.method, request2.method)
    assertEquals(request.name, request2.name)
  }

  @Test def test2() = {
    implicit val baton:Option[AnyRef] = None
    val response = Response(Status.OK, JNothing, JObject(List()))
    val bytes = new ResponseSerializer().toBinary(response)
    println("got serialized response: " + new String(bytes))
    val result = new ResponseSerializer().fromBinary(bytes)
    assertTrue(result.isInstanceOf[Response])
    val response2 = result.asInstanceOf[Response]
    assertEquals(response.status, response2.status)
  }

  @Test def test3 = {
    val obj = ("null" -> JNull) ~ ("bool" -> JBool(true)) ~ ("int" -> JInt(42)) ~ ("double" -> JDouble(3.14159)) ~ ("string" -> JString("foo")) ~ ("array" -> JArray(List())) ~ Nil
    val bytes = SExpSerializer().toBinary(obj)
    println("got bytes " + new String(bytes))
  }
}
