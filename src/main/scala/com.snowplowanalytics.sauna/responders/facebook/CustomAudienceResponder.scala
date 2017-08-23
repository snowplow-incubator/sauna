/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
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
package responders
package facebook

//java
import java.security.MessageDigest

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Try, Failure, Success}
import scala.util.{Either, Right, Left}

// play
import play.api.libs.json._
import play.api.libs.functional.syntax._

// akka
import akka.actor.{ActorRef, Props}

//facebook
import com.facebook.ads.sdk._

// sauna
import Responder._
import loggers.Logger.Notification
import observers.Observer._
import utils.Command
import CustomAudienceResponder._


class CustomAudienceResponder(
  val accessToken: String,
  val appSecret: String,
  val accountId: String,
  val logger: ActorRef) extends Responder[ObserverCommandEvent, CustomAudiencePayloadReceived] {

  val apiContext: APIContext = new APIContext(accessToken, appSecret)
  val account: AdAccount = new AdAccount(accountId, apiContext)

  def extractEvent(observerEvent: ObserverEvent): Option[CustomAudiencePayloadReceived] = {
    observerEvent match {
      case e: ObserverCommandEvent =>
        val commandJson = Json.parse(Source.fromInputStream(e.streamContent).mkString)
        Command.extractCommand[CustomAudiencePayload](commandJson) match {
          case Right((envelope, data)) =>
            Command.validateEnvelope(envelope) match {
              case Right(_) =>
                Some(CustomAudiencePayloadReceived(data, e))
              case Left(error) =>
                notifyLogger(error)
                None
            }
          case Left(error) =>
            notifyLogger(error)
            None
        }
      case _ => None
    }
  }

  /**
   * Upload the custom target audience from the command payload.
   *
   * @param payload The event containing a room notification.
   */
  def process(event: CustomAudiencePayloadReceived): Unit =
    dispatch(event.data) match {
      case Success(message) => context.parent ! CustomAudiencePayloadSent(event, s"Successfully sent HipChat notification: $message")
      case Failure(error) => notifyLogger(s"Error while uploading audience: $error")
    }

  def dispatch(payload: CustomAudiencePayload) = Try{
    val user = new CustomAudience(payload.customAudienceId, apiContext).createUser()
    user.setPayload(payload.payload()).execute()
  }
}

object CustomAudienceResponder {
  /**
   * @param schema  Array specifying the types of the user-identifiable datapoints 
   *        provided in the following data section for each user.
   * @param data   An array of arrays, each inner arrays contain an entry for each 
   *        user to add to the custom audience. Each user entry is itself an array,
   *        containing the user-identifiable datapoints specified in the schema
   */
  case class AudienceUsers(val schema: Seq[String], val data: Seq[Seq[String]]){
    /**
     * Rebuilds the AudienceUsers structure with the data hashed.
     * NB: This method does not check if the data is already hashed
     *
     * @return AudienceUsers
     */
    def rebuildHashed(): AudienceUsers = new AudienceUsers(schema, data.map{ 
      _.map{ key => sha256(key) match{
          case Some(s) => s
          case None => throw new Exception(s"Failed to hash $key")
        }
      }
    })

    def sha256(data: String): Option[String] = Try{
      val hash = MessageDigest.getInstance("SHA-256").digest(data.getBytes("UTF-8"))
      val sb = new StringBuilder
      hash.foreach{b =>
        val hex = Integer.toHexString(0xff & b)
        sb ++= (hex + (if(hex.length == 1) "0" else ""))
      }
      Some(sb.toString)
    } match{
        case Success(s) => s
        case Failure(_) => None
    }
  }

  implicit val audienceUserReads: Reads[AudienceUsers] = (
    (JsPath \ "schema").read[Seq[String]] and
    (JsPath \ "data").read[Seq[Seq[String]]]
  )(AudienceUsers.apply _)

  implicit val audienceWrites: Writes[AudienceUsers] = (
    (JsPath \ "schema").write[Seq[String]] and
    (JsPath \ "data").write[Seq[Seq[String]]]
  )(unlift(AudienceUsers.unapply))

  /**
   * Custom Audience Payload, prehashed
   */
  case class CustomAudiencePayload(
    val customAudienceId: String, val users: AudienceUsers, val preHashed: Boolean
  ){
    /**
     * Prepare the payload to be passed to the Facebook API.
     * Essentially this rebuilds the json received in the command making sure the data is
     * properly formatted, hashed and does not exceed the Facebook APi limits/
     */
    def payload(): String = {
      val _users = if(!preHashed) users.rebuildHashed() else users
      val obj = Json.obj("schema" -> Json.toJson(_users.schema), "data" -> Json.toJson(_users.data))
      Json.stringify(obj)
    }
  }

  implicit val customAudiencePayloadReads: Reads[CustomAudiencePayload] = (
    (JsPath \ "customAudienceId").read[String] and
    (JsPath \ "users").read[AudienceUsers] and
    (JsPath \ "preHashed").read[Boolean]
  ) (CustomAudiencePayload.apply _)

  implicit val customAudiencePayloadWrites: Writes[CustomAudiencePayload] = (
    (JsPath \ "customAudienceId").write[String] and
    (JsPath \ "users").write[AudienceUsers] and
    (JsPath \ "preHashed").write[Boolean]
  )(unlift(CustomAudiencePayload.unapply))

  case class CustomAudiencePayloadReceived(
    data: CustomAudiencePayload,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  /**
   * A responder result denoting that the custom audience payload was successfully sent
   * by the responder.
   *
   * @param source  The responder event that triggered this.
   * @param message A success message.
   */
  case class CustomAudiencePayloadSent(
    source: CustomAudiencePayloadReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[CustomAudienceResponder]] actor.
   *
   * @param config Facebook Custom Audience Config Parameters.
   * @param logger  A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(config: FacebookCustomAudienceConfigParameters_1_0_0, logger: ActorRef): Props =
    props(config.accessToken, config.appSecret, config.accountId, logger)

  def props(accessToken: String, appSecret: String, accountId: String, logger: ActorRef) =
    Props(new CustomAudienceResponder(accessToken, appSecret, accountId, logger))  
}