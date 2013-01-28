package com.memeo.loki

import java.util
import util.Map.Entry
import org.apache.commons.collections.iterators.FilterIterator
import org.apache.commons.collections.Predicate
import collection.convert.WrapAsScala

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/25/13
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */
object Util
{
  def filteredMap[K, V](m:util.Map[K, V], pred:(K, V) => Boolean):util.Map[K, V] = {
    new util.AbstractMap[K, V] {
      def entrySet(): util.Set[Entry[K, V]] = {
        filteredSet(m.entrySet(), (e) => pred(e.getKey, e.getValue))
      }
    }
  }

  def filteredSet[E](s:util.Set[E], pred:(E) => Boolean):util.Set[E] = {
    new util.AbstractSet[E] {
      def size(): Int = {
        var i = 0
        val it = iterator()
        while (it.hasNext) {
          it.next()
          i += 1
        }
        i
      }

      def iterator(): util.Iterator[E] = filteredIterator(s.iterator(), pred)
    }
  }

  def filteredIterator[E](it:util.Iterator[E], pred:(E) => Boolean):util.Iterator[E] = {
    new util.Iterator[E] {
      val i = new FilterIterator(it, new Predicate {
        def evaluate(o:Any): Boolean = pred(o.asInstanceOf[E])
      })

      def hasNext: Boolean = i.hasNext

      def next(): E = i.next().asInstanceOf[E]

      def remove() = i.remove()
    }
  }

  def boundedMap[K, V](m:util.Map[K, V], limit:Int):util.Map[K, V] = {
    return new util.AbstractMap[K, V] {
      def entrySet(): util.Set[Entry[K, V]] = {
        boundedSet(m.entrySet(), limit)
      }
    }
  }

  def boundedSet[E](set:util.Set[E], limit:Int):util.Set[E] = {
    return new util.AbstractSet[E] {
      def size(): Int = math.min(set.size(), limit)

      def iterator(): util.Iterator[E] = boundedIterator(set.iterator(), limit)
    }
  }

  def boundedIterator[E](it:util.Iterator[E], limit:Int):util.Iterator[E] = {
    class Iter(val it:util.Iterator[E]) extends util.Iterator[E] {
      var i = 0

      def hasNext: Boolean = it.hasNext && i < limit

      def next(): E = {
        if (i >= limit) throw new NoSuchElementException()
        else {
          val e = it.next()
          i += 1
          e
        }
      }

      def remove() = throw new UnsupportedOperationException()
    }
    new Iter(it)
  }
}
