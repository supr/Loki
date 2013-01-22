package com.memeo.loki

import java.util.Comparator
import net.liftweb.json.JsonAST._
import com.ibm.icu.text.{RuleBasedCollator, Collator}
import net.liftweb.json.JsonAST.JBool

/**
 * JSON value collation, as is done in http://wiki.apache.org/couchdb/View_collation
 */
class JValueComparator extends Comparator[JValue]
{
  val collator:Collator = new RuleBasedCollator("<\ufffe")

  def compare(o1: JValue, o2: JValue): Int = {
    o1 match {
      case JNothing => o2 match {
        case JNothing => 0
        case _ => -1
      }

      case JNull => o2 match {
        case JNothing => 1
        case JNull => 0
        case _ => -1
      }

      case JBool(false) => o2 match {
        case JNothing|JNull => 1
        case JBool(false) => 0
        case _ => -1
      }

      case JBool(true) => o2 match {
        case JNothing|JNull|JBool(false) => 1
        case JBool(true) => 0
        case _ => -1
      }

      case i:JInt => o2 match {
        case JNothing|JNull|JBool(_) => 1
        case i2:JInt => i.values.compare(i2.values)
        case f:JDouble => i.values.compare(f.values.intValue)
        case _ => -1
      }

      case f:JDouble => o2 match {
        case JNothing|JNull|JBool(_) => 1
        case i:JInt => BigInt(f.values.toInt).compare(i.values)
        case f2:JDouble => f.values.compare(f2.values)
        case _ => -1
      }

      case s:JString => o2 match {
        case JNothing|JNull|JBool(_)|JInt(_)|JDouble(_) => 1
        case s2:JString => collator.compare(s.values, s2.values)
        case _ => -1
      }

      case a:JArray => o2 match {
        case JNothing|JNull|JBool(_)|JInt(_)|JDouble(_)|JString(_) => 1
        case a2:JArray => {
          a.children.zipAll(a2.children, JObject(List()), JObject(List())).foldLeft(0)((result:Int, e:Tuple2[JValue, JValue]) => if (result == 0) compare(e._1, e._2) else result)
        }
        case _ => -1
      }

      case o:JObject => o2 match {
        case JNothing|JNull|JBool(_)|JInt(_)|JDouble(_)|JString(_)|JArray(_) => 1
        case o2:JObject => {
          val r = o.obj.zip(o2.obj).foldLeft(0)((res, e) => if (res == 0) collator.compare(e._1.name, e._2.name) else res)
          if (r == 0) o.obj.length.compareTo(o2.obj.length) else r
        }
      }
    }
  }
}
