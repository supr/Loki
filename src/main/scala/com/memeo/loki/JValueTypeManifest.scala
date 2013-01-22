package com.memeo.loki

import org.mapdb.TypeManifest
import net.liftweb.json.JsonAST.{JNothing, JValue}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/21/13
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
class JValueTypeManifest extends TypeManifest[JValue, JValue]
{
  def keyClass():Class[JValue] = classOf[JValue]
  def valueClass():Class[JValue] = classOf[JValue]
  def defaultKey():JValue = JNothing
  def defaultValue():JValue = JNothing
}
