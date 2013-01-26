package com.memeo.loki

import com.ibm.icu.text.{RuleBasedCollator, Collator}
import java.util.Comparator

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
object StringComparator {
  val collator:Collator = new RuleBasedCollator("<\ufffe")
}

class StringComparator extends Comparator[String] with Serializable {
  def compare(o1: String, o2: String): Int = {
    if (o1.length() > 0 && o1.charAt(0) == '_') {
      if (o2.length() > 0 && o2.charAt(0) == '_') {
        StringComparator.collator.compare(o1, o2)
      }
      else
      {
        1
      }
    }
    else
    {
      if (o2.length() > 0 && o2.charAt(0) == '_') {
        -1
      }
      else
      {
        StringComparator.collator.compare(o1, o2)
      }
    }
  }
}