package com.memeo.loki

import java.security.MessageDigest
import java.nio.charset.Charset
import java.math.BigInteger

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
object Lookup
{
  val utf8 = Charset.forName("UTF-8")
  def hash(name:String, n:Int, i:Int):Int = {
    val md:MessageDigest = MessageDigest.getInstance("MD5")
    md.update(name.getBytes(utf8))
    val k = BigInt(new BigInteger(1, md.digest()))
    val x = k % (1 << i)
    if (x < n)
      (k % (1 << (i + 1))).intValue()
    else
      x.intValue()
  }
}
