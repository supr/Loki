package com.memeo.loki

import com.ibm.icu.text.{RuleBasedCollator, Collator}
import java.util.Comparator

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
class StringComparator extends Comparator[String] with Serializable {
  val collator:Collator = new RuleBasedCollator("<\ufffd")

  def compare(o1: String, o2: String): Int = {
    if (o1.length() > 0 && o1.charAt(0) == '_') {
      if (o2.length() > 0 && o2.charAt(0) == '_') collator.compare(o1, o2)
      else 1
    }
    else
    {
      if (o2.length() > 0 && o2.charAt(0) == '_') -1
      else collator.compare(o1, o2)
    }
  }
}