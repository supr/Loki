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

import org.apache.zookeeper._
import org.apache.zookeeper.AsyncCallback.{StatCallback, DataCallback, ChildrenCallback}
import java.util
import org.apache.zookeeper.data.Stat
import scala.collection.concurrent.TrieMap
import scala.collection.convert.WrapAsScala
import akka.actor.ActorSystem
import akka.event.Logging

class ZookeeperClusterConfig(val zk:ZooKeeper, val me:Int, val url:String)(implicit system:ActorSystem) extends ClusterConfig with Watcher
{
  private val logger = Logging(system, classOf[ZookeeperClusterConfig])
  private var _n:Int = -1
  private var _i:Int = -1
  private val _peers:TrieMap[Int, Member] = new TrieMap[Int, Member]()

  getN()
  getI()
  getPeers()

  zk.setData("/loki/nodes/" + me, url.getBytes(), -1, new StatCallback {
    def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat) = {
      logger.info("registered myself with ZooKeeper id:{} url:{}", me, url)
    }
  }, null)

  def n: Int = _n
  def i: Int = _i

  /**
   * A list of node numbers mapped to members (either self,
   * or a remote peer).
   */
  def peers: Map[Int, Member] = _peers.toMap

  def process(event: WatchedEvent) = {
    logger.info("ZooKeeper event fired {} {}", event.getType, event.getPath)
    event.getType match {
      case Watcher.Event.EventType.NodeDataChanged => {
        event.getPath match {
          case "/loki/n" => {
            logger.info("refreshing n...")
            getN()
          }
          case "/loki/i" => {
            logger.info("refreshing i...")
            getI()
          }
          case s:String if s.startsWith("/loki/nodes/") => {
            val nodeId = s.substring(s.lastIndexOf('/'))
            try {
              getNode(nodeId.toInt)
            } catch {
              case x:Exception => Unit
            }
          }
        }
      }

      case Watcher.Event.EventType.NodeChildrenChanged => {
        getPeers()
      }
    }
  }

  private def getN() = {
    logger.info("refreshing n...")
    zk.getData("/loki/n", this, new DataCallback {
      def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat) = {
        KeeperException.Code.get(rc) match {
          case KeeperException.Code.OK => {
            ZookeeperClusterConfig.this._n = new String(data).toInt
            logger.info("new n={}", ZookeeperClusterConfig.this._n)
          }
          case code:KeeperException.Code => logger.warning("exception reading /loki/n: {}", code)
        }
      }
    }, null)
  }

  private def getI() = {
    logger.info("refreshing i...")
    zk.getData("/loki/i", this, new DataCallback {
      def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat) = {
        KeeperException.Code.get(rc) match {
          case KeeperException.Code.OK => {
            ZookeeperClusterConfig.this._i = new String(data).toInt
            logger.info("new i={}", ZookeeperClusterConfig.this._i)
          }
          case code:KeeperException.Code => logger.warning("exception reading /loki/i: {}", code)
        }
      }
    }, null)
  }

  private def getPeers() = {
    logger.info("refreshing peers...")
    zk.getChildren("/loki/nodes", this, new ChildrenCallback {
      def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]) = {
        KeeperException.Code.get(rc) match {
          case KeeperException.Code.OK => {
            WrapAsScala.collectionAsScalaIterable(children).foreach(peerId => {
              peerId.toInt match {
                case i:Int if i == me => _peers.put(me, Self(me, "loki" + me))
                case i:Int => getNode(i)
              }
            })
          }
          case code:KeeperException.Code => logger.warning("exception reading /loki/nodes: {}", code)
        }
      }
    }, null)
  }

  private def getNode(i:Int) = {
    logger.info("refreshing node {}", i)
    zk.getData("/loki/nodes/" + i, this, new DataCallback {
      def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat) = {
        KeeperException.Code.get(rc) match {
          case KeeperException.Code.OK => {
            val url = new String(data)
            val peer = Peer(url, i, "loki" + i)
            _peers.put(i, peer)
            logger.info("updated peer={}", peer)
          }
          case code:KeeperException.Code => logger.warning("exception refreshing node {}: {}", i, code)
        }
      }
    }, null)
  }
}
