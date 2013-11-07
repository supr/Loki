/*
 * Copyright 2013 Memeo, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.memeo.loki

import akka.actor.{ActorRef, ActorSystem}

abstract class Member(val id:Int, val name:String)
case class Self(override val id:Int, override val name:String) extends Member(id, name)
case class Peer(val ipcAddr:String, override val id:Int, override val name:String) extends Member(id, name)

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
trait ClusterConfig
{
  def n:Int
  def i:Int

  /**
   * A list of node numbers mapped to members (either self,
   * or a remote peer).
   */
  def peers:Map[Int, Member]
}
