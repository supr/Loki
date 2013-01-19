package com.memeo.loki

import net.liftweb.json.JsonAST.{JObject, JValue}
import org.jboss.netty.handler.codec.http.HttpMethod

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
class Request(val method:HttpMethod, val name:String, val value:JValue, val headers:JObject, val params:JObject)
{
}

object Request
{
  def apply(method:HttpMethod, name:String, value:JValue, headers:JObject, params:JObject):Request =
    new Request(method, name, value, headers, params)

  def apply(method:String, name:String, value:JValue, headers:JObject, params:JObject):Request =
    Request(HttpMethod.valueOf(method), name, value, headers, params)
}