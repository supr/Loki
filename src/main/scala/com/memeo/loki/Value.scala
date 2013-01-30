package com.memeo.loki

import java.math.BigInteger
import java.security.MessageDigest
import java.util
import collection.convert.WrapAsScala
import java.nio.charset.Charset
import scala.{Boolean, Array}
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import runtime.RichBoolean
import java.io.{OutputStream, DataOutputStream}
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JBool
import net.liftweb.json.JsonAST.JString

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
sealed abstract class DocumentMember
case object NullMember extends DocumentMember
case class BoolMember(value:Boolean) extends DocumentMember
case class IntMember(value:BigInt) extends DocumentMember
case class DoubleMember(value:Double) extends DocumentMember
case class StringMember(value:String) extends DocumentMember
case class ArrayMember(value:Array[DocumentMember]) extends DocumentMember
case class ObjectMember(value:Map[String, DocumentMember]) extends DocumentMember

class Document(val value:Map[String, DocumentMember])

object Value
{
  def apply(id:String, seq:BigInteger, v:JValue):Value = {
    v match {
      case o:JObject => new Value(id, seq, false,
        new Document(JValueConversion.unpackValue(o).asInstanceOf[ObjectMember].value))
      case _ => throw new IllegalArgumentException("invalid object")
    }
  }
}

class Value(val id:String, val seq:BigInt,
            val deleted:Boolean,
            val value:Document)
{
  def toObject():JObject = {
    val obj = deleted match {
      case true => JObject(List())
      case false => JValueConversion.pack(ObjectMember(value.value))
    }
    val o = obj ~ ("_id" -> JString(id)) ~ ("_rev" -> JString(genRev))
    if (deleted)
      o ~ ("_deleted" -> JBool(true))
    else
      o
  }

  def revHash():String = {
    val md = MessageDigest.getInstance("SHA-1")
    val dos = new DataOutputStream(new OutputStream {
      def write(b: Int) = md.update(b.toByte)
      override def write(b:Array[Byte], offset:Int, length:Int) = md.update(b, offset, length)
    })
    val ser = new ValueSerializer()
    ser.serialize(dos, this)
    md.digest().foldLeft(new StringBuilder)((b, e) => b.append("%02x".format(e & 0xFF))).toString
  }

  def genRev():String = {
    "%d-%s".format(seq, revHash())
  }
}
