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

class StringComparator extends Comparator[String] {
  def compare(o1: String, o2: String): Int = StringComparator.collator.compare(o1, o2)
}