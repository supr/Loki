package com.memeo.loki

import java.math.BigInteger
import java.security.MessageDigest
import java.util
import collection.convert.WrapAsScala
import java.nio.charset.Charset
import scala.{Boolean, Array}
import net.liftweb.json.JsonAST.{JString, JBool, JObject, JValue}
import net.liftweb.json.JsonDSL._
import runtime.RichBoolean

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */

object Value
{
  def apply(id:String, seq:BigInteger, v:JValue):Value = {
    v match {
      case o:JObject => new Value(id, seq, false, JValueConversion.unpack(o).asInstanceOf[util.LinkedHashMap[String, Object]])
      case _ => throw new IllegalArgumentException("invalid object")
    }
  }

  val utf8 = Charset.forName("UTF-8")
  val OBJECT = 0.toByte
  val LIST = 1.toByte
  val NULL = 2.toByte
  val BOOLEAN_TRUE = 3.toByte
  val BOOLEAN_FALSE = 4.toByte
  val INTEGER = 5.toByte
  val DOUBLE = 6.toByte
  val STRING = 7.toByte
  val ROOT = 0xfe.toByte
  val END = 0xff.toByte
}

class Value(val id:String, val seq:BigInteger,
            val deleted:Boolean,
            val value:java.util.LinkedHashMap[String, Object]) extends Serializable
{
  import Value._

  private def hash(md:MessageDigest, o:Any):Unit = {
    o match {
      case null => md.update(NULL)
      case b:Boolean => {
        md.update(if (b) BOOLEAN_TRUE else BOOLEAN_FALSE)
      }
      case s:String => {
        md.update(STRING)
        val len = s.length()
        md.update(Array((len >> 24).toByte,
          (len >> 16).toByte,
          (len >> 8).toByte,
          len.toByte))
        md.update(s.getBytes(utf8))
      }
      case i:BigInteger => {
        md.update(INTEGER)
        md.update(i.toByteArray)
      }
      case d:Double => {
        md.update(DOUBLE)
        md.update("%g".format(d).getBytes(utf8))
      }
      case f:Float => {
        md.update(DOUBLE)
        md.update("%g".format(f).getBytes(utf8))
      }
      case l:util.List[Object] => {
        md.update(LIST)
        val len = l.size()
        md.update(Array((len >> 24).toByte,
          (len >> 16).toByte,
          (len >> 8).toByte,
          len.toByte))
        WrapAsScala.iterableAsScalaIterable(l).foreach(e => hash(md, e))
        md.update(END)
      }
      case m:util.LinkedHashMap[String, Object] => {
        md.update(OBJECT)
        val len = m.size()
        md.update(Array((len >> 24).toByte,
          (len >> 16).toByte,
          (len >> 8).toByte,
          len.toByte))
        WrapAsScala.iterableAsScalaIterable(m.entrySet()).foreach {
          (e) => {
            md.update(e.getKey.getBytes(utf8))
            hash(md, e.getValue)
          }
        }
        md.update(END)
      }
    }
  }

  def hashRoot(md:MessageDigest, r:util.LinkedHashMap[String, Object]) = {
    md.update(ROOT)
    val len = r.size()
    md.update(Array((len >> 24).toByte,
      (len >> 16).toByte,
      (len >> 8).toByte,
      len.toByte))
    WrapAsScala.iterableAsScalaIterable(r.entrySet()).filter(e => !e.getKey.startsWith("_")).foreach(e => {
      md.update(e.getKey.getBytes(utf8))
      hash(md, e.getValue)
    })
  }

  def toObject():JObject = {
    val o = JValueConversion.pack(value).asInstanceOf[JObject] ~ ("_id" -> JString(id)) ~ ("_rev" -> JString(genRev))
    if (deleted)
      o ~ ("_deleted" -> JBool(true))
    else
      o
  }

  def revHash():String = {
    val md = MessageDigest.getInstance("SHA-1")
    hashRoot(md, value)
    md.digest().foldLeft(new StringBuilder)((b, e) => b.append("%02x".format(e & 0xFF))).toString
  }

  def genRev():String = {
    "%d-%s".format(seq, revHash())
  }
}
