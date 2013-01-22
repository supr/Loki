package com.memeo.loki

import org.jboss.netty.channel.{Channels, ChannelPipeline, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.http.{HttpContentCompressor, HttpResponseEncoder, HttpRequestDecoder}
import akka.actor.{ActorRef, Props, ActorSystem}

/**
 * Created with IntelliJ IDEA.
 * User: csm
 * Date: 1/21/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
class HttpServerPipelineFactory(val upstream:ActorRef, val system:ActorSystem) extends ChannelPipelineFactory
{
  def getPipeline: ChannelPipeline = {
    val pl:ChannelPipeline = Channels.pipeline
    pl.addLast("decoder", new HttpRequestDecoder())
    pl.addLast("encoder", new HttpResponseEncoder())
    pl.addLast("deflater", new HttpContentCompressor())
    pl.addLast("handler", new HttpServer(upstream, system))
    pl
  }
}
