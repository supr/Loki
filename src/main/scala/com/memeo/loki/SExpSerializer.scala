package com.memeo.loki

import akka.serialization.Serializer
import net.liftweb.json.JsonAST._
import java.io.{EOFException, IOException, ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.Charset
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.JsonAST.JArray
import scala.collection.mutable.ListBuffer

object SExpSerializer
{
  val utf8 = Charset.forName("UTF-8")
  val OBJ = Array('{'.toByte)
  val ARR = Array('['.toByte)
  val NUL = Array('n'.toByte)
  val INT = Array('i'.toByte)
  val FLT = Array('f'.toByte)
  val STR = Array('s'.toByte)
  val BOL = Array('b'.toByte)
  val END = Array('('.toByte)

  val instance = new SExpSerializer

  def apply():SExpSerializer = instance
}

class EndOfSequence extends IOException

class SExpSerializer extends Serializer
{
  import SExpSerializer._
  def identifier: Int = 0x73657870

  def beginList(out:ByteArrayOutputStream):Unit = {
    out.write('(')
  }

  def endList(out:ByteArrayOutputStream):Unit = {
    out.write(')')
  }

  def writeAtom(out:ByteArrayOutputStream, value:Array[Byte]):Unit = {
    out.write(value.length.toString.getBytes())
    out.write(':')
    out.write(value)
  }

  def writeObject(out:ByteArrayOutputStream, obj:JObject):Unit = {
    beginList(out)
    writeAtom(out, OBJ)
    beginList(out)
    obj.obj.foreach(f => {
      beginList(out)
      writeAtom(out, f.name.getBytes(utf8))
      write(out, f.value)
      endList(out)
    })
    endList(out)
    endList(out)
  }

  def writeArray(out:ByteArrayOutputStream, array:JArray):Unit = {
    beginList(out)
    writeAtom(out, ARR)
    beginList(out)
    array.arr.foreach(f => write(out, f))
    endList(out)
    endList(out)
  }

  def write(out:ByteArrayOutputStream, value:AnyRef):Unit = {
    value match {
      case obj:JObject => writeObject(out, obj)
      case arr:JArray => writeArray(out, arr)
      case str:JString => {
        beginList(out)
        writeAtom(out, STR)
        writeAtom(out, str.values.getBytes(utf8))
        endList(out)
      }
      case i:JInt => {
        beginList(out)
        writeAtom(out, INT)
        writeAtom(out, i.values.toByteArray)
        endList(out)
      }
      case d:JDouble => {
        beginList(out)
        writeAtom(out, FLT)
        val f = d.values
        val s = f"$f"
        writeAtom(out, s.getBytes(utf8))
        endList(out)
      }
      case b:JBool => {
        beginList(out)
        writeAtom(out, BOL)
        writeAtom(out, if (b.values) Array(1.toByte) else Array(0.toByte))
        endList(out)
      }
      case JNull => {
        beginList(out)
        writeAtom(out, NUL)
        endList(out)
      }
      case _ => throw new IllegalArgumentException("can't serialize value: " + value)
    }
  }

  def readObject(in:ByteArrayInputStream, buf:ListBuffer[JField] = ListBuffer[JField]()):JObject = {
    val c = in.read()
    c match {
      case '(' => {
        val key = new String(readAtom(in))
        val value = read(in)
        buf += JField(key, value)
        expect(in, ')')
        readObject(in, buf)
      }
      case ')' => {
        expect(in, ')')
        JObject(buf.result())
      }
      case -1 => throw new EOFException
      case _ => throw new IOException(f"expected ( or ), got $c%02x")
    }
  }

  def readArray(in:ByteArrayInputStream, buf:ListBuffer[JValue] = ListBuffer[JValue]()):JArray = {
    try {
      val value = read(in)
      buf += value
      readArray(in, buf)
    }
    catch {
      case e:EndOfSequence => {
        expect(in, ')')
        JArray(buf.result())
      }
    }
  }

  def read(in:ByteArrayInputStream):JValue = {
    val c = in.read()
    c match {
      case '(' => Unit
      case ')' => throw new EndOfSequence
      case _ => throw new IOException(s"expected (, got $c")
    }
    val atom = readAtom(in)
    if (atom.length != 1)
      throw new IOException("expected 1-byte atom")
    atom(0) match {
      case '{' => {
        expect(in, '(')
        readObject(in)
      }
      case '[' => {
        expect(in, '(')
        readArray(in)
      }
      case 's' => {
        val v = JString(new String(readAtom(in), utf8))
        expect(in, ')')
        v
      }
      case 'i' => {
        val v = JInt(BigInt(readAtom(in)))
        expect(in, ')')
        v
      }
      case 'f' => {
        val v = JDouble(new String(readAtom(in), utf8).toDouble)
        expect(in, ')')
        v
      }
      case 'b' => {
        val v = JBool(readAtom(in)(0) != 0)
        expect(in, ')')
        v
      }
      case 'n' => {
        expect(in, ')')
        JNull
      }
      case _ => throw new IOException("expected type tag")
    }
  }

  def readAtomLength(in:ByteArrayInputStream, value:Int = 0):Int = {
    val c = in.read
    if (c == -1)
      throw new EOFException
    c match {
      case '0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9' => {
        val v = (value * 10) + (c - '0')
        readAtomLength(in, v)
      }
      case ':' => value
      case _ => throw new IOException(s"expecting decimal digit or :, got $c")
    }
  }

  def readAtom(in:ByteArrayInputStream):Array[Byte] = {
    val str = new StringBuilder
    val len = readAtomLength(in)
    val b = new Array[Byte](len)
    if (in.read(b) < len)
      throw new EOFException
    b
  }

  def expect(in:ByteArrayInputStream, value:Byte) = {
    val read = in.read
    if (read == -1)
      throw new EOFException
    if (read.toByte != value)
      throw new IOException(f"expected $value%02x, got $read%02x")
  }

  def toBinary(o: AnyRef): Array[Byte] = {
    val out = new ByteArrayOutputStream
    write(out, o)
    out.toByteArray
  }

  def includeManifest: Boolean = false

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
    read(new ByteArrayInputStream(bytes))
}
