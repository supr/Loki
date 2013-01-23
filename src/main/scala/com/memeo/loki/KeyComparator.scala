package com.memeo.loki

import java.util.{Collections, Comparator}
import com.ibm.icu.text.{RuleBasedCollator, Collator}
import java.math.BigInteger
import java.math
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

object KeyComparator
{
  val collator:Collator = new RuleBasedCollator("<\ufffe")
}

/**
 * JSON value collation, as is done in http://wiki.apache.org/couchdb/View_collation
 */
class KeyComparator extends Comparator[Object] with Serializable
{
  import collection.convert.WrapAsScala.{mapAsScalaMap, iterableAsScalaIterable}
  import KeyComparator.collator

  def number2bigdec(n:Number):java.math.BigDecimal = {
    n match {
      case bd:java.math.BigDecimal => bd
      case bi:BigInteger       => new java.math.BigDecimal(bi)
      case d:java.lang.Double  => new math.BigDecimal(d.doubleValue())
      case f:java.lang.Float   => new math.BigDecimal(f.doubleValue())
      case l:java.lang.Long    => new math.BigDecimal(l.longValue())
      case i:java.lang.Integer => new math.BigDecimal(i.intValue())
      case s:java.lang.Short   => new math.BigDecimal(s.intValue())
      case al:AtomicLong       => new math.BigDecimal(al.longValue())
      case ai:AtomicInteger    => new math.BigDecimal(ai.intValue())
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

      case a:java.util.List[Object] => o2 match {
        case null|_:java.lang.Boolean|_:java.lang.Number|_:String => 1
        case a2:java.util.List[Object] => {
          iterableAsScalaIterable(a).zipAll(iterableAsScalaIterable(a2),
            Collections.emptyMap(), Collections.emptyMap()).foldLeft(0)((result:Int, e:(Object, Object)) =>
              if (result == 0) compare(e._1, e._2) else result)
        }
        case _ => -1
      }

      case m1:java.util.Map[String, Object] => o2 match {
        case null|_:java.lang.Boolean|_:java.lang.Number|_:String|_:java.util.List[Object] => 1
        case m2:java.util.Map[String, Object] => {
          val r = mapAsScalaMap(m1).zip(mapAsScalaMap(m2)).foldLeft(0)((res:Int, e:((String,Object),(String,Object))) =>
            if (res == 0) collator.compare(e._1._1, e._2._1) else res)
          if (r == 0) m1.size().compareTo(m2.size()) else r
        }
        case _ => -1
      }

      case _ => o2 match {
        case null|_:java.lang.Boolean|_:java.lang.Number|_:String|_:java.util.List[Object]|_:java.util.Map[String, Object] => 1
        case _ => collator.compare(o1.toString, o2.toString)
      }
    }
  }
}
