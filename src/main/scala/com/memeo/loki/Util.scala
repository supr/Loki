package com.memeo.loki

import java.util
import util.Map.Entry

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/25/13
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */
object Util
{
  def boundedMap[K, V](m:util.Map[K, V], limit:Int):util.Map[K, V] = {
    return new util.AbstractMap[K, V] {
      def entrySet(): util.Set[Entry[K, V]] = {
        return new util.AbstractSet[Entry[K, V]] {
          def size(): Int = Math.min(m.size(), limit)

          def iterator(): util.Iterator[Entry[K, V]] = {
            val it = m.entrySet().iterator()
            return new util.Iterator[Entry[K, V]]
            {
              var i = 0
              def next(): Entry[K, V] = {
                if (i >= limit) throw new NoSuchElementException
                else {
                  val next = it.next()
                  i += 1
                  next
                }
              }

              def remove() = throw new UnsupportedOperationException()

              def hasNext: Boolean = {
                return it.hasNext && i < limit
              }
            }
          }
        }
      }
    }
  }
}
