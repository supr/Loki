package com.memeo.loki

import java.util.Comparator
import net.liftweb.json.JsonAST._
import com.ibm.icu.text.{RuleBasedCollator, Collator}
import net.liftweb.json.JsonAST.JBool
import java.math.BigInteger
import java.math
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

/**
 * JSON value collation, as is done in http://wiki.apache.org/couchdb/View_collation
 */
class ValueComparator extends Comparator[Object]
{
  val collator:Collator = new RuleBasedCollator("<\ufffe")

  def number2bigdec(n:Number):java.math.BigDecimal = {
    n match {
      case bd:java.math.BigDecimal => bd
      case bi:BigInteger => new java.math.BigDecimal(bi)
      case d:java.lang.Double => new math.BigDecimal(d.doubleValue())
      case f:java.lang.Float => new math.BigDecimal(f.doubleValue())
      case l:java.lang.Long => new math.BigDecimal(l.longValue())
      case i:java.lang.Integer => new math.BigDecimal(i.intValue())
      case s:java.lang.Short => new math.BigDecimal(s.intValue())
      case al:AtomicLong => new math.BigDecimal(al.longValue())
      case ai:AtomicInteger => new math.BigDecimal(ai.intValue())
    }
  }

  def compare(o1: Object, o2: Object): Int = {
    o1 match {
      case null => o2 match {
        case null => 0
        case _ => -1
      }

      case b1:java.lang.Boolean => o2 match {
        case null => 1
        case b2:java.lang.Boolean => b1.compareTo(b2)
        case _ => -1
      }

      case n1:java.lang.Number => o2 match {
        case null|_:java.lang.Boolean => 1
        case n2:java.lang.Number => number2bigdec(n1).compareTo(number2bigdec(n2))
        case _ => -1
      }

      case s1:String => o2 match {
        case null|_:java.lang.Boolean|_:java.lang.Number => 1
        case s2:String => collator.compare(s1, s2)
        case _ => -1
      }

      case a:List[Any] => o2 match {
        case null|_:java.lang.Boolean|_:java.lang.Number|_:String => 1
        case a2:Seq[Any] => {
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
