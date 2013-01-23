package com.memeo.loki

abstract class Member(val id:Int, val name:String)
case class Self(override val id:Int, override val name:String) extends Member(id, name)
case class Peer(val ipcAddr:String, override val id:Int, override val name:String) extends Member(id, name)

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
trait ClusterConfig
{
  val n:Int
  val i:Int

  /**
   * A list of node numbers mapped to members (either self,
   * or a remote peer).
   */
  val peers:Map[Int, Member]
}
