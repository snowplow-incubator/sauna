/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.sauna
package apis

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Try, Success, Failure}


// akka
import akka.actor.ActorRef

// play
import play.api.libs.json._
import play.api.libs.ws.WSResponse

// jackson
import com.fasterxml.jackson.core.JsonParseException

// sauna
import loggers.Logger.Notification
import utils._

/**
 * Pusher API wrapper. Encapsulates all communications with Pusher
 *
 * @param token 	app id.
 * @param appKey 	api key
 * @param appSecret 	api secret
 * @param cluster  	cluster
 * @param logger 	A logger actor.
 */
 class Pusher(appId: String, appKey: String, appSecret: String, cluster: Option[String], logger: ActorRef){
 	import Pusher._
	import scala.collection.convert.WrapAsJava

	val pusher: PusherAPI =  Try(new PusherAPI(appId, appKey, appSecret, cluster)) match {
		case Failure(err) =>
			logger ! Notification(s"Invalid configuration passed to PusherAPI. - ${err.getMessage()}")
			throw new Exception("Failed to load PusherAPI")
		case Success(p) => p
	}

 	/**
	 * Publish an event to one or more Pusher channels.
	 *
	 * @param event Pusher Event
	**/
	def publishEvent(event: Pusher.Event): Future[Event.Result]  = Future{
		val socketId: String = event.socketId.getOrElse(null)
		import Event.Result

		val channels = WrapAsJava.seqAsJavaList(event.channels.getOrElse(Nil).toSeq)
		Result from{ () =>
			pusher.trigger(channels, event.name, event.data.json, socketId)
		}
	}
 }

 object Pusher{
	import play.api.libs.json._ // JSON library
	import play.api.libs.json.Reads._ // Custom validation helpers
	import play.api.libs.functional.syntax._ // Combinator syntax
	//import com.pusher.rest.Pusher

	class PusherAPI(appId: String, key: String, secret: String, cluster: Option[String]) extends com.pusher.rest.Pusher(appId, key, secret){
		//data is already be serialiased
		override def serialise(data: Any): String = data.toString

		cluster match{
			case Some(c) => setCluster(c)
			case None => ()
		}
	}

 	object Event{
 		type Name = String
		type SocketID = String
		type Channel = String
		sealed case class Data(json: JsValue)

		abstract class Result(val status: Result.Status, message: String, httpResponse: Int)
		object Result{
			class Status(s: com.pusher.rest.data.Result.Status){
				def isSuccessful = s == com.pusher.rest.data.Result.Status.SUCCESS
				override def toString = s.toString
			}

			import com.pusher.rest.data.Result.Status
			def from(fn: () => com.pusher.rest.data.Result)  = {
				val result = Try(fn()) match{
					case Success(r) => r
					case Failure(err) => throw new Exception(err)
				}

				val message = result.getMessage
				val httpResponse = result.getHttpStatus
				val status = new Status(result.getStatus)

				new Result(status, message, httpResponse){
					override def toString = s"Result(HTTP/$httpResponse, $status , $message)"
				}
			}
		}

 	}

	sealed case class Event(val name: String, val data: Event.Data, val channels: Option[Seq[Event.Channel]] = None, socketId: Option[Event.SocketID] = None)

	implicit val readsEvent :Reads[Event] = (
		(JsPath \ "event").read[Event.Name] and
		(JsPath \ "data").read[JsObject].map(v => Event.Data(v)) and
		(JsPath \ "channels").readNullable[Seq[Event.Channel]] and
		(JsPath \ "socketId").readNullable[Event.SocketID]
	)(Event.apply _)

 }
