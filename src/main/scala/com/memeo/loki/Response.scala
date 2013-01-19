package com.memeo.loki

import net.liftweb.json.JsonAST.{JObject, JValue}
import org.jboss.netty.handler.codec.http.HttpResponseStatus

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/16/13
 * Time: 4:46 PM
 * To change this template use File | Settings | File Templates.
 */
class Response(val status:HttpResponseStatus, val value:JValue, val headers:JObject)
{
}

object Response
{
  def apply(status:HttpResponseStatus, value:JValue, headers:JObject):Response = new Response(status, value, headers)
}
